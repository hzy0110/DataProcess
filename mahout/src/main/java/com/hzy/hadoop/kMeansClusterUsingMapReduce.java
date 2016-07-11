package com.hzy.hadoop;

import com.hzy.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

/**
 * Created by Hzy on 2016/7/11.
 */


public class kMeansClusterUsingMapReduce {

    private static final String HDFS = PropertiesUtil.getValue("hdfs");

    public static void main(String[] args) throws Exception {
        // 声明一个计算距离的方法，这里选择了欧几里德距离
        DistanceMeasure measure = new EuclideanDistanceMeasure();

        String localFile = "/home/hzy/tmp/mahout/randomData.csv";
        String inPath = HDFS + "/mahout/hdfs/mix_data";
        String seqFile = inPath + "/seqfile";
        String seeds = inPath + "/seeds";
        String outPath = inPath + "/result/";
        String clusteredPoints = outPath + "/clusteredPoints";
        Configuration conf = HdfsDAO.config();
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(inPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(localFile, inPath);
        hdfs.ls(inPath);

        // 指定需要聚类的个数，这里选择 2 类
        int k = 2;

        // 指定 K 均值聚类算法的最大迭代次数
        int maxIter = 3;

        // 指定 K 均值聚类算法的最大距离阈值
        double distanceThreshold = 0.01;

        // 随机的选择k个作为簇的中心
        Path seqFilePath = new Path(seqFile);
        Path clustersSeeds = new Path(seeds);
        clustersSeeds = RandomSeedGenerator.buildRandom(conf, seqFilePath, clustersSeeds, k, measure);

        // 调用 KMeansDriver.runJob 方法执行 K 均值聚类算法
        KMeansDriver.run(conf, seqFilePath, clustersSeeds, new Path(outPath), 0.01, 10, true, 0.01, false);

        // 调用 ClusterDumper 的 printClusters 方法将聚类结果打印出来。
        Path outGlobPath = new Path(outPath, "clusters-*-final");
        Path clusteredPointsPath = new Path(clusteredPoints);
        System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath, clusteredPointsPath);


        ClusterDumper clusterDumper = new ClusterDumper(outGlobPath, clusteredPointsPath);
        clusterDumper.printClusters(null);
    }
}