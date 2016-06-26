package com.zmyuan.spark.hw10;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by zhudebin on 16/6/25.
 */
public class No1 {

    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
        Connection con = null; //表示数据库的连接对象
        Statement stmt = null;
        Class.forName("org.apache.hive.jdbc.HiveDriver"); //1、使用CLASS 类加载驱动程序
        con = DriverManager.getConnection("jdbc:hive2://192.168.137.160:10000", "squid", "squid"); //2、连接数据库
        stmt = con.createStatement(); //3、Statement 接口需要通过Connection 接口进行实例化操作
        //        ResultSet rs = stmt.executeQuery("select ds,count(ds) from invites group by ds");
        ResultSet rs = stmt.executeQuery("show tables");
        while(rs.next()) {
            String lid = rs.getString(1);
            //            String lid2 = rs.getString(2);
            //            System.out.println(lid + "\t" + lid2);
            System.out.println(lid);
        }
        con.close(); // 4、关闭数据库
        System.out.println(System.currentTimeMillis());
    }
}
