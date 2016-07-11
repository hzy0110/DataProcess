package com.hzy.hadoop;

import com.hzy.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Hzy on 2016/4/1.
 */
public class HdfsDAO {

    //HDFS访问地址
    private static final String HDFS = PropertiesUtil.getValue("hdfs");

    public HdfsDAO(Configuration conf) {
        this(HDFS,conf);
    }

    public HdfsDAO(String hdfs,Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf =  conf;
    }

    //private static final String HDFS = "hdfs://192.168.70.128:8020/";


    /*public HdfsDAO() {

        this.conf =  config();
    }*/

    //hdfs路径
    private String hdfsPath;
    //Hadoop系统配置
    private Configuration conf =  config();

    //启动函数
    public static void main(String[] args) throws IOException {
        HdfsDAO hdfs = new HdfsDAO(config());
        hdfs.mkdirs("/tmp/new/two");
        hdfs.ls("/tmp/new");
    }

    //加载Hadoop配置文件
    public static Configuration config(){
        Configuration conf = new Configuration();
        conf.addResource(Thread.currentThread().getContextClassLoader().getResource("hadoop/core-site.xml").getFile());
        conf.addResource(Thread.currentThread().getContextClassLoader().getResource("hadoop/hdfs-site.xml").getFile());
        conf.addResource(Thread.currentThread().getContextClassLoader().getResource("hadoop/mapred-site.xml").getFile());
        return conf;
    }



    public void ls(String folder) throws IOException {
        Path path = new Path(folder);

        this.getClass().getResource("/hadoop/core-site.xml");

        String cc = conf.get("fs.default.name");
        System.out.println("cc: " + cc);
        //conf.addResource("classpath:/hadoop/core-site.xml").;

        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }

    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

//    public void copyFile(String local, String remote) throws IOException {
//        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
//        fs.copyToLocalFile(new Path(local), new Path(remote));
//        System.out.println("copy from: " + local + " to " + remote);
//        fs.close();
//    }


    public void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try {
            fsdis =fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }


    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }


}
