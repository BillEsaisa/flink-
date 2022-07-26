package com.atguigu.utils;


import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql) {


        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //preparedStatement => insert into t values(?,?,?,?,?,?)
                        //通过反射的方式获取所有的字段
                        Class<?> clz = t.getClass();
                        //Field[] fields = clz.getFields();
                        Field[] fields = clz.getDeclaredFields();

                        int j = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];

                            field.setAccessible(true);

                            //获取字段上的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                j++;
                                continue;
                            }

                            //获取值
                            Object value = field.get(t);

                            preparedStatement.setObject(i + 1 - j, value);
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());


    }
}
