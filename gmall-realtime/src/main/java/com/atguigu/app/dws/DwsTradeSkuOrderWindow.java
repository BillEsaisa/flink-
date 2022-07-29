package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeSkuOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

public class DwsTradeSkuOrderWindow {

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

        //TODO 2.读取Kafka DWD层 下单主题数据创建流
        String topic = "dwd_trade_order_detail";
        String groupId = "sku_order_window_220212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.转换为JSON对象并提取事件戳
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception ignored) {
                }
            }
        });

        //TODO 4.按照订单明细ID分组
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));

        //TODO 5.去重,由left join产生的重复数据
        SingleOutputStreamOperator<JSONObject> filterLeftJoinDS = keyedByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                //获取状态中的数据
                JSONObject lastValue = valueState.value();

                if (lastValue == null) {
                    valueState.update(value);
                    TimerService timerService = ctx.timerService();
                    long ts = timerService.currentProcessingTime();
                    timerService.registerProcessingTimeTimer(ts + 5000L);
                } else if (value.getString("row_op_ts").compareTo(lastValue.getString("row_op_ts")) >= 0) {
                    valueState.update(value);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();
            }
        });

        //TODO 6.转换为JavaBean对象
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderDS = filterLeftJoinDS.map(json -> {

            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(json.getString("order_id"));

            Double split_activity_amount = json.getDouble("split_activity_amount");
            if (split_activity_amount == null) {
                split_activity_amount = 0.0D;
            }

            Double split_coupon_amount = json.getDouble("split_coupon_amount");
            if (split_coupon_amount == null) {
                split_coupon_amount = 0.0D;
            }

            return TradeSkuOrderBean.builder()
                    .skuId(json.getString("sku_id"))
                    .skuName(json.getString("sku_name"))
                    .orderIds(orderIds)
                    .originalAmount(json.getDouble("split_original_amount"))
                    .activityAmount(split_activity_amount)
                    .couponAmount(split_coupon_amount)
                    .orderAmount(json.getDouble("split_total_amount"))
                    .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                    .build();
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeSkuOrderBean>() {
            @Override
            public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 7.分组、开窗、聚合
        KeyedStream<TradeSkuOrderBean, String> keyedBySkuDS = tradeSkuOrderDS.keyBy(TradeSkuOrderBean::getSkuId);
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = keyedBySkuDS.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.getOrderIds().addAll(value2.getOrderIds());
                        value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                        value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                        value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {

                        TradeSkuOrderBean next = input.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setOrderCount((long) next.getOrderIds().size());

                        out.collect(next);
                    }
                });

        //打印测试
        reduceDS.print("reduceDS>>>>>>>>>>");

        //TODO 8.关联维表补充维度信息

        //TODO 9.将数据写出到ClickHouse

        //TODO 10.启动任务
        env.execute("DwsTradeSkuOrderWindow");

    }

}
