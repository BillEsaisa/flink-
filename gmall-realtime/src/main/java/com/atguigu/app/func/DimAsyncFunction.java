package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.JedisPoolUtil;
import com.atguigu.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T> {

    private JedisPool jedisPool;
    private DruidDataSource druidDataSource;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedisPool = JedisPoolUtil.getJedisPool();
        druidDataSource = DruidDSUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {

                //获取连接
                Jedis jedis = jedisPool.getResource();
                DruidPooledConnection connection = druidDataSource.getConnection();

                //查询维表
                String key = getKey(input);
                JSONObject dimInfo = DimUtil.getDimInfo(jedis, connection, tableName, key);

                //补充信息
                if (dimInfo != null) {
                    join(input, dimInfo);
                }

                //归还连接
                jedis.close();
                connection.close();

                //输出补充完信息的数据
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
