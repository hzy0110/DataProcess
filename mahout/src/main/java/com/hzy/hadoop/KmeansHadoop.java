package com.hzy.hadoop;

/**
 * Created by Hzy on 2016/4/2.
 */

import com.hzy.util.PropertiesUtil;
import com.hzy.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class KmeansHadoop {
    private static final String HDFS = PropertiesUtil.getValue("hdfs");

    public static void main(String[] args) throws Exception {
        InputDriverRunnable inputDriverRunnable = new InputDriverRunnable();
        kMeansClusterUsingMapReduce kMeansClusterUsingMapReduce = new kMeansClusterUsingMapReduce();
        ReadCluster readCluster =new ReadCluster();
        ReadSeq readSeq =new ReadSeq();



        String localFile =HDFS +  "/mahout/reuters-sparse/tfidf-vectors";
        String inPath = HDFS + "/mahout/hdfs/mix_data2";
        String seqFile = inPath + "/seqfile";
        String seeds = inPath + "/seeds";
        String outPath = inPath + "/result/";
        String clusteredPoints = outPath + "clusteredPoints";
        String clusters = "clusters-*-final";
        Configuration conf = HdfsDAO.config();
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(inPath);
        hdfs.mkdirs(inPath);
        //hdfs.copyFile(localFile, inPath);
        hdfs.ls(inPath);

        String in = "";
        String out = "";
        String select_value = "";
        String outcluster = "";

        //in = "H:/testdata/randomData.csv";
        //out = HDFS + "/mahout/inputdeiveout";
        select_value = "org.apache.mahout.math.RandomAccessSparseVector";
        //String[] s = {localFile, seqFile, select_value};
        //inputDriverRunnable.setArgs(s);
        //inputDriverRunnable.run();

//mahout kmeans -i /mahout/reuters-sparse/tfidf-vectors -c /mahout/reuters-kmeans-clusters -o /reuters-kmeans -k 20 -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 200 -ow --clustering
        //in  = HDFS +"/mahout/reuters-sparse/tfidf-vectors";
        //out = HDFS +"/mahout/kmeans/"+"resule";

        String k = "3";
        String convergenceDelta = "0.1";
        String maxIter = "10";
        String clustering = "true";
        String distanceMeasure = "org.apache.mahout.common.distance.CosineDistanceMeasure";
        String[] parKM = {localFile,outPath,HDFS + "/mahout/hdfs/" + clusters,k,convergenceDelta,maxIter,clustering,distanceMeasure};
        kMeansClusterUsingMapReduce.setArgs(parKM);
        kMeansClusterUsingMapReduce.run();

//
        Path outGlobPath = new Path(outPath, clusters);
        Path clusteredPointsPath = new Path(clusteredPoints);
        System.out.println("outGlobPath=  " + outGlobPath + "  clusteredPointsPath= " + clusteredPointsPath);



        //in = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusters-3-final";
        //outcluster = "/home/hzy/tmp/mahout/out/cluster2.dat";
        outcluster = outPath + "cluster2.dat";
        //String points = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints/part-m-00000";
        //String points = clusteredPoints;
        distanceMeasure = "org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure";
        String include_per_cluster = "-1";
        System.out.println("outGlobPath.toString() =" + outGlobPath.toString());
        String[] parCD = {outGlobPath.toString(),outcluster,clusteredPoints,distanceMeasure,include_per_cluster};
        readCluster.setArgs(parCD);
        readCluster.runJob();



        //-i hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints  -o ./reuters-kmeans-seqdumper2
        //String[] sfd = {"-i","hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints","-o","./reuters-kmeans-seqdumper2"};
        //in = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints/part-m-00000";
        //out = "/home/hzy/tmp/mahout/out/seq2.dat";
        out = outPath + "seq2.dat";
        String sp = "\n";
        String[] parSFD = {clusteredPoints,out,sp};
        readSeq.setArgs(parSFD);
        readSeq.runJob();

    }
}