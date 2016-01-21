package com.hzy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

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
        readFile(fs, file);

        fs.close();

    }

    /**
     * 创建文件夹
     * @param fs
     * @param path
     * @throws IOException
     */
    public static void mkDir(FileSystem fs,Path path) throws IOException{
        fs.create(path);
    }

    /**
     * 永久性删除指定的文件或目录，如果f是一个空目录或者文件，那么recursive的值就会被忽略。只有recursive＝true时，一个非空目录及其内容才会被删除。
     * @param fs
     * @param path
     * @param recursive 永久性删除指定的文件或目录，如果f是一个空目录或者文件，那么recursive的值就会被忽略。只有recursive＝true时，一个非空目录及其内容才会被删除。
     * @throws IOException
     */
    public static boolean delDir(FileSystem fs,Path path,boolean recursive) throws IOException{
        return fs.delete(path, recursive);
    }

    /**
     * 重命名文件夹
     * @param fs
     * @param path
     * @param renamePath
     * @return
     * @throws IOException
     */
    public static boolean renameDir(FileSystem fs,Path path,Path renamePath)throws IOException{
        return fs.rename(path, renamePath);
    }

    public static void writeFile(FileSystem fs,Path path,String context)throws IOException{
        FSDataOutputStream out = fs.create(path);
        out.writeUTF(context);
    }

    /**
     * 读取文件内容
     * @param fs
     * @param path
     * @throws IOException
     */
    public static void readFile(FileSystem fs,Path path) throws IOException{
        if (!fs.exists(path)) {
            System.out.println("File does not exists");
            return;
        }

        FSDataInputStream in = fs.open(path);
        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String s = "";
        while ((s = d.readLine()) != null) {
            System.out.println(s);
        }
        d.close();

    }

    /**
     * 上传一个本地文件
     * @throws IOException
     */
    public void putLocalFile1(FileSystem fs,Path srcpath,Path destpath) throws IOException{
        fs.copyFromLocalFile(srcpath, destpath);
    }

    /**
     * 查看某个文件夹下面的所有文件
     * @param fs
     * @param path
     * @throws IOException
     */
    public static void getFileSingle(FileSystem fs,Path path) throws IOException {
        FileStatus[] files = fs.listStatus(path);
        for (FileStatus file : files) {
            System.out.println(file.getPath().toString());
        }
    }

    /**
     * 获取给定目录下的所有子目录以及子文件
     * @param fs
     * @param path
     * @throws IOException
     */
    public static void getFileMulti(FileSystem fs,Path path) throws IOException {
        FileStatus[] files = fs.listStatus(path);
        for(int i=0;i<files.length;i++){
            if(files[i].isDirectory()){
                Path p = new Path(files[i].getPath().toString());
                getFileMulti(fs,p);
            }else{
                System.out.println(files[i].getPath().toString());
            }
        }
    }

    /**
     * 查看某个文件的数据块信息
     * @param fs
     * @param path
     * @throws Exception
     */
    public static void getBlockInfo(FileSystem fs,Path path)throws IOException{
        FileStatus filestatus = fs.getFileStatus(path);
        BlockLocation[] blkLoc = fs.getFileBlockLocations
                (filestatus, 0, filestatus.getLen());
        for (BlockLocation loc : blkLoc) {
            for (int i = 0; i < loc.getHosts().length; i++) {
                //获取数据块在哪些主机上
                System.out.println(loc.getHosts()[i]);//获取文件块的主机名
                //由于这个文件只有一个块，所以输出结果为:slave2、slave1、slave5
            }
        }
    }


    /**
     * HDFS集群上所有节点名称信息
     * @Title: aaa
     * @Description: bbb
     * @param fs
     * @return
     * @throws
     */
    public static void getHDFSNode(FileSystem fs) throws IOException{

        DistributedFileSystem dfs = (DistributedFileSystem)fs;
        DatanodeInfo[] dataNodeStats = dfs.getDataNodeStats();

        for(int i=0;i<dataNodeStats.length;i++){
            System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
        }
    }

}
