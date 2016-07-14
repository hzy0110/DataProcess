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



        String localFile =HDFS +  "/mahout/inputdata/randomData.csv";
        String inPath = HDFS + "/mahout/hdfs/mix_data1";
        String seqFile = inPath + "/seqfile";
        String seeds = inPath + "/seeds";
        String outPath = inPath + "/result/";
        String clusteredPoints = outPath + "/clusteredPoints";
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
        String[] s = {localFile, seqFile, select_value};
        inputDriverRunnable.setArgs(s);
        inputDriverRunnable.run();


        //in  = HDFS +"/mahout/reuters-sparse/tfidf-vectors";
        //out = HDFS +"/mahout/kmeans/"+"resule";
        String clusters = "clusters-*-final";
        String k = "3";
        String convergenceDelta = "0.1";
        String maxIter = "10";
        String clustering = "true";
        String distanceMeasure = "org.apache.mahout.common.distance.CosineDistanceMeasure";
        String[] parKM = {seqFile,outPath,clusters,k,convergenceDelta,maxIter,clustering,distanceMeasure};
        kMeansClusterUsingMapReduce.setArgs(parKM);
        kMeansClusterUsingMapReduce.run();


        Path outGlobPath = new Path(outPath, clusters);
        Path clusteredPointsPath = new Path(clusteredPoints);
        System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath, clusteredPointsPath);


        ClusterDumper clusterDumper = new ClusterDumper();
        //in = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusters-3-final";
        outcluster = "/home/hzy/tmp/mahout/out/cluster.dat";
        //String points = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints/part-m-00000";
        //String points = clusteredPoints;
        distanceMeasure = "org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure";
        String include_per_cluster = "-1";
        //System.out.printf("outGlobPath.toString() =", outGlobPath.toUri().getPath());
        String[] parCD = {outGlobPath.toString(),outcluster,clusteredPoints,distanceMeasure,include_per_cluster};
        readCluster.setArgs(parCD);
        readCluster.runJob();



        //-i hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints  -o ./reuters-kmeans-seqdumper2
        //String[] sfd = {"-i","hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints","-o","./reuters-kmeans-seqdumper2"};
        //in = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints/part-m-00000";
        out = "/home/hzy/tmp/mahout/out/seq.dat";
        String sp = "\n";
        String[] parSFD = {clusteredPoints,out,sp};
        readSeq.setArgs(parSFD);
        readSeq.runJob();

    }
}