package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流：web/app -> nginx -> 日志服务器(File) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//程  序：Mock_log -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)
public class DwdTrafficUserJumpDetail {

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
        String groupId = "user_jump_detail_0212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));

        //TODO 3.转换为JSON对象&过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("脏数据：" + value);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

//        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                return value.getJSONObject("page").getString("last_page_id") == null;
//            }
//        }).times(2).consecutive().within(Time.seconds(10));

        //TODO 6.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件(注意超时事件提取)
        OutputTag<String> outputTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(outputTag,
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.get("start").get(0).toJSONString();
                    }
                }, new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("start").get(0).toJSONString();
                    }
                });
        DataStream<String> timeOutDS = selectDS.getSideOutput(outputTag);

        //TODO 8.合并两个流
        DataStream<String> unionDS = selectDS.union(timeOutDS);

        //TODO 9.将数据写出到Kafka
        selectDS.print("selectDS>>>>>>");
        timeOutDS.print("timeOutDS>>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        //TODO 10.启动任务
        env.execute("DwdTrafficUserJumpDetail");

    }

}
