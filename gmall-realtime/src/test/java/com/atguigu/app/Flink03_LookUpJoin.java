package com.atguigu.app;

import com.atguigu.bean.Bean1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Flink03_LookUpJoin {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0], split[1], Long.parseLong(split[2]));
                });
        //将流注册为表,同时获取处理时间
        tableEnv.createTemporaryView("t1", bean1DS,
                $("id"),
                $("name"),
                $("ts"),
                $("pt").proctime());

        //构建LookUp表
        tableEnv.executeSql("" +
                "CREATE TEMPORARY TABLE base_dic ( " +
                "  dic_code STRING, " +
                "  dic_name STRING, " +
                "  parent_code STRING, " +
                "  PRIMARY KEY (dic_code) NOT ENFORCED " +
                ") WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall-220212-flink', " +
                "  'lookup.cache.max-rows' = '10', " +
                "  'lookup.cache.ttl' = '1 hour', " +
                "  'table-name' = 'base_dic', " +
                "  'username' = 'root', " +
                "  'password' = '000000' " +
                ")");

        //使用FlinkSQL关联事实表与维度表
        tableEnv.sqlQuery("" +
                "select " +
                "    id, " +
                "    name, " +
                "    dic_name " +
                "from " +
                "    t1 " +
                "join " +
                "    base_dic FOR SYSTEM_TIME AS OF t1.pt as dic " +
                "on " +
                "    t1.id=dic.dic_code").execute().print();

    }

}
