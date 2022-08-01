package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeSkuOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
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
import java.util.concurrent.TimeUnit;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwsTradeSkuOrderWindow(Phoenix,Redis) -> ClickHouse(ZK)
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

        //TODO 8.关联维表补充维度信息  SKU  SPU  TM  Category3  Category2  Category1
//        reduceDS.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
//            @Override
//            public void open(Configuration parameters) throws Exception {
//            }
//            @Override
//            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {
//                //通过sku_id查询补充信息
//                //通过spu_id查询补充信息
//                //通过tm_id查询补充信息
//                //通过Category3查询补充信息
//                //通过Category2查询、补充信息
//                //通过Category1查询补充信息
//                return null;
//            }
//        });

        //8.1 关联sku_info
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                        input.setSpuId(dimInfo.getString("SPU_ID"));
                    }
                },
                100, TimeUnit.SECONDS);

        //8.2 关联spu_info
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderWithSpuDS = AsyncDataStream.unorderedWait(
                tradeSkuOrderWithSkuDS,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        input.setSpuName(dimInfo.getString("SPU_NAME"));
                    }
                },
                100, TimeUnit.SECONDS);

        //tradeSkuOrderWithSpuDS.print("SpuDS>>>>>>>>>>>");

        //8.3 关联base_trademark
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderWithTmDS = AsyncDataStream.unorderedWait(
                tradeSkuOrderWithSpuDS,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //8.4 关联category3
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderWithCategory3DS = AsyncDataStream.unorderedWait(
                tradeSkuOrderWithTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                },
                100, TimeUnit.SECONDS);

        //8.5 关联category2
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderWithCategory2DS = AsyncDataStream.unorderedWait(
                tradeSkuOrderWithCategory3DS,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                },
                100, TimeUnit.SECONDS);

        //8.6 关联category1
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderWithCategory1DS = AsyncDataStream.unorderedWait(
                tradeSkuOrderWithCategory2DS,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                },
                100, TimeUnit.SECONDS);

        //TODO 9.将数据写出到ClickHouse
        tradeSkuOrderWithCategory1DS.print(">>>>>>>>>>>>>");
        tradeSkuOrderWithCategory1DS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_sku_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 10.启动任务
        env.execute("DwsTradeSkuOrderWindow");

    }

}
