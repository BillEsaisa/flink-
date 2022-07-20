package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> stateDescriptor;
    private DruidDataSource druidDataSource;

    public TableProcessFunction() {
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    //Value:{"before":null,"after":{"id":6,"tm_name":"长粒香","logo_url":"/static/default.jpg"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1658192885916,"snapshot":"false","db":"gmall-211227-flink","sequence":null,"table":"base_trademark","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1658192885916,"transaction":null}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //1.获取并将数据转换为JavaBean对象
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        //3.写入状态
        String key = tableProcess.getSourceTable();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        broadcastState.put(key, tableProcess);
    }

    //校验并建表 create table if not exists db.tn(id varchar primary key,name varchar,sex varchar) xxx
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        DruidPooledConnection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            //创建建表SQL语句
            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }

                //如果不是最后一个字段,那么需要拼接","
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }
            sql.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(">>>>>" + sql);

            //编译SQL
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql.toString());

            //执行写入
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("创建:" + sinkTable + "失败！");
        } finally {
            //释放资源
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //value:{"database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":100924,"user_id":"93","sku_id":16,"cart_price":4488,"sku_num":1,"img_url":"http://47.93.148.192:8080/group1/M0rBHu8l-sklaALrngAAHGDqdpFtU741.jpg","sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万30倍数字变焦 8GB+128GB亮黑色全网通5G手机","is_checked":null,"create_time":"2020-06-14 09:28:57","operate_time":null,"is_ordered":1,"order_time":"2021-10-17 09:28:58","source_type":"2401","source_id":null},"old":{"is_ordered":0,"order_time":null}}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //1.获取广播数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));
        String type = value.getString("type");

        //2.过滤数据  行
        if (tableProcess != null && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type))) {

            //过滤数据 列
            filterColumns(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.补充SinkTable字段写出
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);
        } else {
            System.out.println("未找到对应的配置信息:" + value.getString("table") + ",或者类型错误:" + type);
        }
    }

    //过滤数据 列
    private void filterColumns(JSONObject data, String sinkColumns) {

        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));

    }

}
