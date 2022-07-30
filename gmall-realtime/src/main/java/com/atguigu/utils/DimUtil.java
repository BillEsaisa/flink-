package com.atguigu.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Jedis jedis, Connection connection, String tableName, String key) throws Exception {

        //读取Redis中的维表数据
        String redisKey = "DIM:" + tableName + ":" + key;
        String dimInfoStr = jedis.get(redisKey);
        if (dimInfoStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            return JSON.parseObject(dimInfoStr);
        }

        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + key + "'";

        //查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfo = queryList.get(0);

        //将从Phoenix查询到的数据写入Redis
        jedis.set(redisKey, dimInfo.toJSONString());
        //设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);

        //返回结果
        return dimInfo;
    }

    public static void main(String[] args) throws Exception {

        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();

        JedisPool jedisPool = JedisPoolUtil.getJedisPool();
        Jedis jedis = jedisPool.getResource();

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(jedis, connection, "DIM_BASE_TRADEMARK", "13")); //148 148 143 148 140
        long end = System.currentTimeMillis();

        System.out.println(getDimInfo(jedis, connection, "DIM_BASE_TRADEMARK", "13")); //10 8 8 9   0 1 1 1 1
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);

        jedis.close();
        jedisPool.close();
        connection.close();
        dataSource.close();
    }

}
