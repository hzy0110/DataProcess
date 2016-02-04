package com.hzy.hive;


import java.sql.*;

/**
 * Created by zy on 2016/2/1.
 */
public class HiveTest {


    private static String driverName =
            "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args)
            throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection(
                "jdbc:hive2://localhost:10000/default", "hzy", "");
        Statement stmt = con.createStatement();
        String tableName = "hzy";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName +
                " (name string, min float, max float, avg float, total int) " +
                " ROW FORMAT DELIMITED " +
                " FIELDS TERMINATED BY '\t' " +
                " STORED AS TEXTFILE ");

        System.out.println("Create table success!");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        System.out.println("show tables  ok");

        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        System.out.println("describe ok");

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        String filepath = "/tmp/out7/part-r-00000";

        //有local表示是本地文件
        //无local表示是hdfs文件
        //sql = "load data local inpath '" + filepath + "' into table " + tableName;
        sql = "load data inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);
        System.out.println("load ok");

        // select * query
        sql = "select * from " + tableName;
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t"
                    + res.getString(2)+ res.getString(3)+ res.getString(4)+ res.getString(5));
        }

        System.out.println("select 1 ok");

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        System.out.println("select 2 ok");
    }

}
