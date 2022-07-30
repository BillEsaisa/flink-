package com.atguigu.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 封装为任何JDBC数据库的任何查询都可以使用该工具类的方法
 * id为主键
 * select count(*) from t;                 单行单列
 * select * from t where id = '1001';      单行多列
 * select count(*) from t group by tm_id;  多行单列
 * select * from t;                        多行多列
 */
public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        //1.创建集合用于存放结果数据
        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //2.预编译SQL
            preparedStatement = connection.prepareStatement(sql);

            //3.执行SQL查询
            resultSet = preparedStatement.executeQuery();

            //4.遍历查询结果集,将每行数据封装为T对象并放入集合
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            //4.1 行遍历
            while (resultSet.next()) {

                //构建T对象
                T t = clz.newInstance();

                //4.2 列遍历
                for (int i = 0; i < columnCount; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object value = resultSet.getObject(columnName);

                    //给T对象赋值
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }
                    BeanUtils.setProperty(t, columnName, value);
                }

                //将T对象加入集合
                list.add(t);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        //5.返回结果
        return list;
    }

    public static void main(String[] args) throws Exception {

        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();

        List<JSONObject> queryList = queryList(connection,
                "select * from GMALL220212_REALTIME.DIM_BASE_TRADEMARK where id='13'",
                JSONObject.class,
                true);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }

        connection.close();
    }
}
