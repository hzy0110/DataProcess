package com.hzy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.hzy.utils.HdfsUtil;

import java.io.*;
import java.net.URI;

/**
 * Created by Hzy on 2016/1/21.
 */
public class Test {
    public static String hdfsUrl = "hdfs://master:8020";
    public static String pathfile = "/tmp/hdfs/test.txt";
    public static String path = "/tmp/hdfs";

    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);
        Path file = new Path(pathfile);
        HdfsUtil.readFile(fs, file);
        HdfsUtil.createWriteFile(fs,file,"test");
        fs.close();

    }

}
