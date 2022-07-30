package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.JedisPoolUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource;
    private JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
        jedisPool = JedisPoolUtil.getJedisPool();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        //拼接SQL upsert into db.tn(id,name,sex) values('1001','zs','male')
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        String sql = genUpsertSql(
                sinkTable,
                data);
        System.out.println(sql);

        //如果为更新操作,则先删除Redis中的数据
        if ("update".equals(value.getString("type"))) {
            Jedis jedis = jedisPool.getResource();
            DimUtil.delDimInfo(jedis,
                    sinkTable.toUpperCase(),
                    data.getString("id"));
            jedis.close();
        }

        //编译&执行
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
        connection.commit();

        //释放资源
        preparedStatement.close();
        connection.close();
    }

    //upsert into db.tn(id,name,sex) values('1001','zs','male')
    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> columns = data.keySet();//columns.mkString(",") ==> ["a","b","c"] -> "a,b,c"
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
