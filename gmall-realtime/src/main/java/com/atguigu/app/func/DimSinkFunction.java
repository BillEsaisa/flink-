package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        //拼接SQL upsert into db.tn(id,name,sex) values('1001','zs','male')
        String sql = genUpsertSql(
                value.getString("sinkTable"),
                value.getJSONObject("data"));
        System.out.println(sql);

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
