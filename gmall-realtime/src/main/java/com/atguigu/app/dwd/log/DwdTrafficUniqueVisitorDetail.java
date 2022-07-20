package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//数据流：web/app -> nginx -> 日志服务器(File) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//程  序：Mock_log -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
public class DwdTrafficUniqueVisitorDetail {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //状态后端设置
        //        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //        env.getCheckpointConfig().enableExternalizedCheckpoints(
        //                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        //        );
        //        env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        //        ));
        //        env.setStateBackend(new HashMapStateBackend());
        //        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取Kafka页面主题数据创建流
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "unique_visitor_detail_0212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));

        //TODO 3.转换为JSON对象&过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据：" + value);
                }
            }
        });

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程进行去重处理
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-dt", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(stateTtlConfig);
                lastDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //提取状态中的数据以及当前数据中的日期
                String lastDt = lastDtState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));

                //如果状态为null或者状态日期与当前日期不同,则保留数据,更新状态
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastDtState.update(curDt);
                    return true;
                } else {
                    return false;
                }
            }
        });

        //TODO 6.将数据写出到Kafka
        filterDS.print(">>>>>>>>");
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        filterDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        //TODO 7.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");

    }

}
