package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;


//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdUserRegister -> Kafka(ZK)
public class DwdUserRegister {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 2. 启用状态后端
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

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "order_detail_220212"));

        // TODO 4. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select " +
                "data['id'] user_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'user_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 5. 创建 Kafka-Connector dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`( " +
                "`user_id` string, " +
                "`date_id` string, " +
                "`create_time` string, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getInsertKafkaDDL("dwd_user_register"));

        // TODO 6. 将输入写入 Kafka-Connector 表
        tableEnv.executeSql("insert into dwd_user_register " +
                "select  " +
                "user_id, " +
                "date_format(create_time, 'yyyy-MM-dd') date_id, " +
                "create_time, " +
                "ts " +
                "from user_info");
    }
}
