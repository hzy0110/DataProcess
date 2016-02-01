package com.hzy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Hzy on 2016/2/1.
 */
public class HBaseTest {
    static Configuration cfg = HBaseConfiguration.create();

    //通过HBaseAdmin HTableDescriptor来创建一个新表
    public static void create(String tableName, String columnFamily) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)) {
            System.out.println("Table exist");
            System.exit(0);
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDescriptor);
            System.out.println("Table create success");
        }
    }

    //添加一条数据，通过HTable Put为已存在的表添加数据
    public static void put(String tableName, String row, String columnFamily, String column, String data) throws IOException {
        HTable table = new HTable(cfg, tableName);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));

        table.put(put);
        System.out.println("put success");
    }

    //获取tableName表里列为row的结果集
    public static void get(String tableName, String row) throws IOException {
        HTable table = new HTable(cfg, tableName);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        System.out.println("get " + result);
    }

    //通过HTable Scan来获取tableName表的所有数据信息
    public static void scan(String tableName) throws IOException {
        HTable table = new HTable(cfg, tableName);
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result s : resultScanner) {
            System.out.println("Scan " + s);
        }
    }

    //根据表明删除表
    public static boolean delete(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }


    public static void main(String[] args) {
        String tableName = "hbase_test";
        String columnFamily = "c1";

        try {
            //HBaseTest.create(tableName, columnFamily);
            System.out.println("-----------put----------");
            HBaseTest.put(tableName, "row1", columnFamily, "column2", "data1");
            HBaseTest.put(tableName, "row1", columnFamily, "column3", "data1");
            HBaseTest.put(tableName, "row2", columnFamily, "column2", "data1");
            HBaseTest.put(tableName, "row2", columnFamily, "column3", "data1");
            System.out.println("-----------get----------");
            HBaseTest.get(tableName, "row1");
            HBaseTest.get(tableName, "row2");
            System.out.println("-----------scan----------");
            HBaseTest.scan(tableName);
            /*if (HBaseTest.delete(tableName) == true) {
                System.out.println("delete table " + tableName + "success");
            }*/

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
}
