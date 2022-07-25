package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MySqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK)
public class DwdTradeOrderPreProcess {

    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "905 s");

//        // TODO 2. 启用状态后端
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

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` String, " +
                "`table` String, " +
                "`type` String, " +
                "`data` map<String, String>, " +
                "`old` map<String, String>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "order_pre_process_220212"));

        // TODO 4. 读取订单明细表数据
        Table orderDetail = tableEnv.sqlQuery("select  " +
                "data['id'] id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['sku_name'] sku_name, " +
                "data['create_time'] create_time, " +
                "data['source_id'] source_id, " +
                "data['source_type'] source_type, " +
                "data['sku_num'] sku_num, " +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " +
                "data['split_total_amount'] split_total_amount, " +
                "data['split_activity_amount'] split_activity_amount, " +
                "data['split_coupon_amount'] split_coupon_amount, " +
                "ts od_ts, " +
                "proc_time " +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // TODO 5. 读取订单表数据
        Table orderInfo = tableEnv.sqlQuery("select  " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['province_id'] province_id, " +
                "data['operate_time'] operate_time, " +
                "data['order_status'] order_status, " +
                "`type`, " +
                "`old`, " +
                "ts oi_ts " +
                "from `topic_db` " +
                "where `table` = 'order_info' " +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 6. 读取订单明细活动关联表数据
        Table orderDetailActivity = tableEnv.sqlQuery("select  " +
                "data['order_detail_id'] order_detail_id, " +
                "data['activity_id'] activity_id, " +
                "data['activity_rule_id'] activity_rule_id " +
                "from `topic_db` " +
                "where `table` = 'order_detail_activity' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // TODO 7. 读取订单明细优惠券关联表数据
        Table orderDetailCoupon = tableEnv.sqlQuery("select " +
                "data['order_detail_id'] order_detail_id, " +
                "data['coupon_id'] coupon_id " +
                "from `topic_db` " +
                "where `table` = 'order_detail_coupon' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // TODO 8. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MySqlUtil.getBaseDic());

        // TODO 9. 关联五张表获得订单明细表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "od.id, " +
                "od.order_id, " +
                "oi.user_id, " +
                "oi.order_status, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "oi.province_id, " +
                "act.activity_id, " +
                "act.activity_rule_id, " +
                "cou.coupon_id, " +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id, " +
                "od.create_time, " +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                "oi.operate_time, " +
                "od.source_id, " +
                "od.source_type, " +
                "dic.dic_name source_type_name, " +
                "od.sku_num, " +
                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount, " +
                "oi.`type`, " +
                "oi.`old`, " +
                "od.od_ts, " +
                "oi.oi_ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_detail od  " +
                "join order_info oi " +
                "on od.order_id = oi.id " +
                "left join order_detail_activity act " +
                "on od.id = act.order_detail_id " +
                "left join order_detail_coupon cou " +
                "on od.id = cou.order_detail_id " +
                "join `base_dic` for system_time as of od.proc_time as dic " +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 10. 建立 Upsert-Kafka dwd_trade_order_pre_process 表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "order_status string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "operate_date_id string, " +
                "operate_time string, " +
                "source_id string, " +
                "source_type string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "`type` string, " +
                "`old` map<string,string>, " +
                "od_ts string, " +
                "oi_ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        // TODO 11. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_pre_process  " +
                "select * from result_table");

        //env.execute();
    }
}
