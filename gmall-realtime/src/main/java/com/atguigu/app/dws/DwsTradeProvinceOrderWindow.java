package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeProvinceOrderWindow;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwsTradeProvinceOrderWindow(Redis,Phoenix) -> ClickHouse(ZK)
public class DwsTradeProvinceOrderWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //启用状态后端
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(
//                RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L))
//        );
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        //TODO 2.读取Kafka ODS层 topic_db 业务数据
        String topic = "topic_db";
        String groupId = "province_order_220212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤数据  去下单数据
        SingleOutputStreamOperator<String> filterDS = kafkaDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return "order_info".equals(jsonObject.getString("table")) && "insert".equals(jsonObject.getString("type"));
            }
        });

        //TODO 4.将数据转换为JavaBean对象
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceOrderDS = filterDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject data = jsonObject.getJSONObject("data");

            return TradeProvinceOrderWindow.builder()
                    .provinceId(data.getString("province_id"))
                    .orderCount(1L)
                    .orderAmount(data.getDouble("total_amount"))
                    .ts(DateFormatUtil.toTs(data.getString("create_time"), true))
                    .build();
        });

        //TODO 5.分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDS = tradeProvinceOrderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
            @Override
            public long extractTimestamp(TradeProvinceOrderWindow element, long recordTimestamp) {
                return element.getTs();
            }
        }))
                .keyBy(TradeProvinceOrderWindow::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> input, Collector<TradeProvinceOrderWindow> out) throws Exception {

                        TradeProvinceOrderWindow next = input.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);
                    }
                });
        reduceDS.print("reduceDS>>>>>>>>");

        //TODO 6.关联维表补充信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceOrderWithProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindow input, JSONObject dimInfo) {
                        input.setProvinceName(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //TODO 7.将数据写出到ClickHouse
        tradeProvinceOrderWithProvinceDS.print(">>>>>>>>>>>>>>>");
        tradeProvinceOrderWithProvinceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("DwsTradeProvinceOrderWindow");

    }


}
