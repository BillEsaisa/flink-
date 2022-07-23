package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MySqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeCartAdd -> Kafka(ZK)
public class DwdTradeCartAdd {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        //TODO 2.读取Kafka topic_db 主题数据创建原始表  注意提取处理时间
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add_220212"));

        //测试
        //tableEnv.sqlQuery("select * from topic_db").execute().print();

        //TODO 3.过滤出加购数据
        Table cartInfo = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['cart_price'] cart_price, " +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num, " +
                "    `data`['sku_name'] sku_name, " +
                "    `data`['is_checked'] is_checked, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['is_ordered'] is_ordered, " +
                "    `data`['order_time'] order_time, " +
                "    `data`['source_type'] source_type, " +
                "    `data`['source_id'] source_id, " +
                "    `pt` " +
                "from topic_db " +
                "where `database`='gmall-220212-flink' " +
                "and `table`='cart_info' " +
                "and ( " +
                "    `type`='insert'  " +
                "    or  " +
                "    (`type`='update'  " +
                "      and  " +
                "      `old`['sku_num'] is not null  " +
                "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("cart_info", cartInfo);

        //打印测试
        //tableEnv.toAppendStream(cartInfo, Row.class).print();

        //TODO 4.构建MySQL base_dic 的LookUp表
        tableEnv.executeSql(MySqlUtil.getBaseDic());
        //打印测试
        //tableEnv.sqlQuery("select * from base_dic").execute().print();

        //TODO 5.关联加购与维表(维度退化)
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    c.id, " +
                "    c.user_id, " +
                "    c.sku_id, " +
                "    c.cart_price, " +
                "    c.sku_num, " +
                "    c.sku_name, " +
                "    c.is_checked, " +
                "    c.create_time, " +
                "    c.operate_time, " +
                "    c.is_ordered, " +
                "    c.order_time, " +
                "    c.source_type, " +
                "    dic.dic_name source_name, " +
                "    c.source_id " +
                "from cart_info c " +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt as dic " +
                "on c.source_type=dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //打印测试
        tableEnv.toAppendStream(resultTable, Row.class).print(">>>>>>>");

        //TODO 6.构建DWD层加购事实表
        tableEnv.executeSql("" +
                "create table dwd_trade_cart_add( " +
                "    `id` STRING, " +
                "    `user_id` STRING, " +
                "    `sku_id` STRING, " +
                "    `cart_price` STRING, " +
                "    `sku_num` STRING, " +
                "    `sku_name` STRING, " +
                "    `is_checked` STRING, " +
                "    `create_time` STRING, " +
                "    `operate_time` STRING, " +
                "    `is_ordered` STRING, " +
                "    `order_time` STRING, " +
                "    `source_type` STRING, " +
                "    `source_name` STRING, " +
                "    `source_id` STRING " +
                ") " + MyKafkaUtil.getInsertKafkaDDL("dwd_trade_cart_add"));

        //TODO 7.写出数据到Kafka
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from result_table");

        //TODO 8.启动任务
        env.execute("DwdTradeCartAdd");
    }

}
