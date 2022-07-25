package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MySqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwdTradePayDetailSuc -> Kafka(ZK)
public class DwdTradePayDetailSuc {

    public static void main(String[] args) throws Exception {

        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "905 s");

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 读取 Kafka dwd_trade_order_detail 主题数据，封装为 Flink SQL 表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "pay_detail_suc_220212"));

        // TODO 4. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` String, " +
                "`table` String, " +
                "`type` String, " +
                "`data` map<String, String>, " +
                "`old` map<String, String>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_pay_detail_suc"));

        // TODO 5. 筛选支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`proc_time`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'payment_info' " +
                "and `type` = 'update' " +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // TODO 6. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MySqlUtil.getBaseDic());

        // TODO 7. 关联 3 张表获得支付成功宽表
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "od.id order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type payment_type_code, " +
                "dic.dic_name payment_type_name, " +
                "pi.callback_time, " +
                "od.source_id, " +
                "od.source_type_code, " +
                "od.source_type_name, " +
                "od.sku_num, " +
                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount split_payment_amount, " +
                "pi.ts, " +
                "od.row_op_ts row_op_ts " +
                "from dwd_trade_order_detail od " +
                "join payment_info pi " +
                "on pi.order_id = od.order_id " +
                "join `base_dic` for system_time as of pi.proc_time as dic " +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8. 创建 Kafka dwd_trade_pay_detail 表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc( " +
                "order_detail_id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "callback_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_payment_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(order_detail_id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        // TODO 9. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_pay_detail_suc select * from result_table");
    }
}

