package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.util.OutputTag;

//数据流：web/app -> nginx -> 日志服务器(File) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock_log -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {

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

        //TODO 2.读取Kafka topic_log 主题数据创建流
        String topic = "topic_log";
        String groupId = "base_log_app_220212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤脏数据&转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        jsonObjDS.getSideOutput(dirtyTag).print("Dirty>>>>>>>");

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程实现新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first-dt", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取数据中的标记以及时间戳、获取状态数据
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                String firstDt = valueState.value();

                if ("1".equals(isNew)) {

                    if (firstDt == null) {
                        //更新状态
                        valueState.update(curDt);
                    } else if (!firstDt.equals(curDt)) {
                        //更新标记为"0"
                        value.getJSONObject("common").put("is_new", "0");
                    }

                } else if (firstDt == null) {
                    //更新状态为昨日
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    valueState.update(yesterday);
                }

                //返回结果数据
                return value;
            }
        });

        //TODO 6.使用侧输出流进行分流处理
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                //尝试获取"err"字段
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errorTag, value.toJSONString());
                    value.remove("err");
                }

                //尝试获取"start"字段
                String start = value.getString("start");
                if (start != null) {
                    ctx.output(startTag, value.toJSONString());
                } else {

                    JSONObject common = value.getJSONObject("common");
                    JSONObject page = value.getJSONObject("page");
                    Long ts = value.getLong("ts");

                    //尝试获取"displays"
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        //遍历写出
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page", page);
                            display.put("ts", ts);

                            //写出到曝光侧输出流中
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    //尝试获取"actions"
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        //遍历写出
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page", page);

                            //写出到曝光侧输出流中
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    //移除曝光和动作数据
                    value.remove("displays");
                    value.remove("actions");

                    out.collect(value.toJSONString());
                }
            }
        });

        //TODO 7.提取各个流的数据&写出到Kafka
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        pageDS.print("Page>>>>");
        startDS.print("Start>>>>");
        displayDS.print("Display>>>>");
        actionDS.print("Action>>>>");
        errorDS.print("Error>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        //TODO 8.启动任务
        env.execute("BaseLogApp");

    }

}
