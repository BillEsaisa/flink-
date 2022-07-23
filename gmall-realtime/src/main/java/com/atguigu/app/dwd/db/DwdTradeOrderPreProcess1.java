package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MySqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess1 -> Kafka(ZK)
public class DwdTradeOrderPreProcess1 {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));

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

        //TODO 2.读取Kafka topic_db主题的数据创建原始表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("order_pre_process_220212"));

        //TODO 3.过滤出订单明细表
        Table orderDetail = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['order_id'] order_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['sku_name'] sku_name, " +
                "    `data`['order_price'] order_price, " +
                "    `data`['sku_num'] sku_num, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['source_type'] source_type, " +
                "    `data`['source_id'] source_id, " +
                "    `data`['split_total_amount'] split_total_amount, " +
                "    `data`['split_activity_amount'] split_activity_amount, " +
                "    `data`['split_coupon_amount'] split_coupon_amount, " +
                "    pt " +
                "from topic_db " +
                "where `database`='gmall-220212-flink' " +
                "and `table`='order_detail' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        //打印测试
        //tableEnv.toAppendStream(orderDetail, Row.class).print();

        //TODO 4.过滤出订单表
        Table orderInfo = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['consignee'] consignee, " +
                "    `data`['consignee_tel'] consignee_tel, " +
                "    `data`['total_amount'] total_amount, " +
                "    `data`['order_status'] order_status, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['payment_way'] payment_way, " +
                "    `data`['delivery_address'] delivery_address, " +
                "    `data`['order_comment'] order_comment, " +
                "    `data`['out_trade_no'] out_trade_no, " +
                "    `data`['trade_body'] trade_body, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['expire_time'] expire_time, " +
                "    `data`['process_status'] process_status, " +
                "    `data`['tracking_no'] tracking_no, " +
                "    `data`['parent_order_id'] parent_order_id, " +
                "    `data`['province_id'] province_id, " +
                "    `data`['activity_reduce_amount'] activity_reduce_amount, " +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    `data`['original_total_amount'] original_total_amount, " +
                "    `data`['feight_fee'] feight_fee, " +
                "    `data`['feight_fee_reduce'] feight_fee_reduce, " +
                "    `data`['refundable_time'] refundable_time, " +
                "    `old` " +
                "from topic_db " +
                "where `database`='gmall-220212-flink' " +
                "and `table`='order_info' " +
                "and (`type`='insert' or `type`='update')");
        tableEnv.createTemporaryView("order_info", orderInfo);

        //打印测试
        //tableEnv.toAppendStream(orderInfo, Row.class).print();

        //TODO 5.过滤出订单明细活动表
        Table orderActivity = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id,  " +
                "    `data`['order_id'] order_id,  " +
                "    `data`['order_detail_id'] order_detail_id,  " +
                "    `data`['activity_id'] activity_id,  " +
                "    `data`['activity_rule_id'] activity_rule_id,  " +
                "    `data`['sku_id'] sku_id,  " +
                "    `data`['create_time'] create_time " +
                "from topic_db " +
                "where `database`='gmall-220212-flink' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_activity", orderActivity);

        //打印测试
        //tableEnv.toAppendStream(orderActivity, Row.class).print("orderActivity>>>");

        //TODO 6.过滤出订单明细购物券表
        Table orderCoupon = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['order_id'] order_id, " +
                "    `data`['order_detail_id'] order_detail_id, " +
                "    `data`['coupon_id'] coupon_id, " +
                "    `data`['coupon_use_id'] coupon_use_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['create_time'] create_time " +
                "from topic_db " +
                "where `database`='gmall-220212-flink' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_coupon", orderCoupon);

        //打印测试
        //tableEnv.toAppendStream(orderCoupon, Row.class).print("orderCoupon>>>>");

        //TODO 7.构建MySQL的 base_dic LookUp表
        tableEnv.executeSql(MySqlUtil.getBaseDic());

        //TODO 8.5表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.id, " +
                "    od.order_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.order_price, " +
                "    od.sku_num, " +
                "    od.create_time, " +
                "    od.source_type, " +
                "    dic.dic_name source_name, " +
                "    od.source_id, " +
                "    od.split_total_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    oi.consignee, " +
                "    oi.consignee_tel, " +
                "    oi.total_amount, " +
                "    oi.order_status, " +
                "    oi.user_id, " +
                "    oi.payment_way, " +
                "    oi.delivery_address, " +
                "    oi.order_comment, " +
                "    oi.out_trade_no, " +
                "    oi.trade_body, " +
                "    oi.operate_time, " +
                "    oi.expire_time, " +
                "    oi.process_status, " +
                "    oi.tracking_no, " +
                "    oi.parent_order_id, " +
                "    oi.province_id, " +
                "    oi.activity_reduce_amount, " +
                "    oi.coupon_reduce_amount, " +
                "    oi.original_total_amount, " +
                "    oi.feight_fee, " +
                "    oi.feight_fee_reduce, " +
                "    oi.refundable_time, " +
                "    oi.`old`, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oc.coupon_id, " +
                "    oc.coupon_use_id " +
                "from order_detail od " +
                "join order_info oi " +
                "on od.order_id=oi.id " +
                "left join order_activity oa " +
                "on od.id=oa.order_detail_id " +
                "left join order_coupon oc " +
                "on od.id=oc.order_detail_id " +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic " +
                "on od.source_type=dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //打印测试
        tableEnv.toChangelogStream(resultTable).print("resultTable>>>>");

        //TODO 9.构建Kafka Upsert表
        tableEnv.executeSql("" +
                "create table dwd_order_pre( " +
                "    `id` STRING, " +
                "    `order_id` STRING, " +
                "    `sku_id` STRING, " +
                "    `sku_name` STRING, " +
                "    `order_price` STRING, " +
                "    `sku_num` STRING, " +
                "    `create_time` STRING, " +
                "    `source_type` STRING, " +
                "    `source_name` STRING, " +
                "    `source_id` STRING, " +
                "    `split_total_amount` STRING, " +
                "    `split_activity_amount` STRING, " +
                "    `split_coupon_amount` STRING, " +
                "    `consignee` STRING, " +
                "    `consignee_tel` STRING, " +
                "    `total_amount` STRING, " +
                "    `order_status` STRING, " +
                "    `user_id` STRING, " +
                "    `payment_way` STRING, " +
                "    `delivery_address` STRING, " +
                "    `order_comment` STRING, " +
                "    `out_trade_no` STRING, " +
                "    `trade_body` STRING, " +
                "    `operate_time` STRING, " +
                "    `expire_time` STRING, " +
                "    `process_status` STRING, " +
                "    `tracking_no` STRING, " +
                "    `parent_order_id` STRING, " +
                "    `province_id` STRING, " +
                "    `activity_reduce_amount` STRING, " +
                "    `coupon_reduce_amount` STRING, " +
                "    `original_total_amount` STRING, " +
                "    `feight_fee` STRING, " +
                "    `feight_fee_reduce` STRING, " +
                "    `refundable_time` STRING, " +
                "    `old` MAP<STRING, STRING>, " +
                "    `activity_id` STRING, " +
                "    `activity_rule_id` STRING, " +
                "    `coupon_id` STRING, " +
                "    `coupon_use_id` STRING, " +
                "    PRIMARY KEY (id) NOT ENFORCED " +
                ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        //TODO 10.将数据写出到Kafka
        tableEnv.executeSql("insert into dwd_order_pre select * from result_table");

        //TODO 11.启动任务
        env.execute("DwdTradeOrderPreProcess1");

    }

}
