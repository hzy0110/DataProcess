package com.hzy.hadoop;

/**
 * Created by Hzy on 2016/4/2.
 */

import com.hzy.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.*;
import org.apache.mahout.utils.clustering.*;

public class KmeansHadoop1 {
    private static final String HDFS = PropertiesUtil.getValue("hdfs");

    public static void main(String[] args) throws Exception {
        String localFile = "/home/hzy/tmp/mahout/input/randomData.csv";
        String inPath = HDFS + "/mahout/hdfs/mix_data1";
        String seqFile = inPath + "/seqfile";
        String seeds = inPath + "/seeds";
        String outPath = inPath + "/result";
        String clusteredPoints = outPath + "/clusteredPoints";
        Configuration conf = HdfsDAO.config();
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(inPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(localFile, inPath);
        hdfs.ls(inPath);

        InputDriver.runJob(new Path(inPath), new Path(seqFile), "org.apache.mahout.math.RandomAccessSparseVector");

        int k = 3;
        Path seqFilePath = new Path(seqFile);
        Path clustersSeeds = new Path(seeds);
        DistanceMeasure measure = new EuclideanDistanceMeasure();
        clustersSeeds = RandomSeedGenerator.buildRandom(conf, seqFilePath, clustersSeeds, k, measure);
        KMeansDriver.run(conf, seqFilePath, clustersSeeds, new Path(outPath), 0.1, 10, true, 0.1, false);


        Path outGlobPath = new Path(outPath, "clusters-*-final");
        Path clusteredPointsPath = new Path(clusteredPoints);
        System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath, clusteredPointsPath);

        org.apache.mahout.utils.clustering.ClusterDumper clusterDumper = new org.apache.mahout.utils.clustering.ClusterDumper(outGlobPath, clusteredPointsPath);
        //org.apache.mahout.utils.clustering.ClusterDumper clusterDumper = new org.apache.mahout.utils.clustering.ClusterDumper();
        String outcluster = "/home/hzy/tmp/mahout/out/cluster1.dat";

        System.out.println("clusteredPoints:" + clusteredPoints);
        System.out.println("outGlobPath:" + outGlobPath.toString());
        System.out.println("outGlobPath:" + outGlobPath.toUri().getPath());
        //System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath, clusteredPointsPath);


        //String[] cd = {"-i","hdfs://master:8020/mahout/hdfs/mix_data/result/clusters-*-final","-o",outcluster,"-of","TEXT","-p",clusteredPoints,"-dm","org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure","--tempDir","temp"};
        String[] cd = {"-i",outGlobPath.toString(),"-o",outcluster,"-of","TEXT","-p",clusteredPoints,"-dm","org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure","--tempDir","temp"};
        clusterDumper.run(cd);



        //clusterDumper.getOption("-o", outPath + "/clusterDumper.txt");
        //clusterDumper.printClusters(null);

        org.apache.mahout.utils.SequenceFileDumper sequenceFileDumper = new org.apache.mahout.utils.SequenceFileDumper();
        //-i hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints  -o ./reuters-kmeans-seqdumper2
        //String[] sfd = {"-i","hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints","-o","./reuters-kmeans-seqdumper2"};
        String out = "/home/hzy/tmp/mahout/out/seq1.dat";
        String[] sfd = {"-i",clusteredPoints,"-o",out};
        sequenceFileDumper.run(sfd);

    }
}