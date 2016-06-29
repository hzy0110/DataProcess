package com.hzy.self;

import com.hzy.self.HdfsDAO;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

public class ItemCFHadoop {

    private static final String HDFS = "hdfs://192.168.70.128:8020";

    public static void main(String[] args) throws Exception {
        String localFile = "/home/hzy/tmp/mahout/item.csv";
        //String localFile = "E:/item.csv";
        String inPath = HDFS + "/user/hdfs/userCF";
        String inFile = inPath + "/item.csv";
        String outPath = HDFS + "/user/hdfs/userCF/result/";
        String outFile = outPath + "/part-r-00000";
        String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();


        HdfsDAO hdfs = new HdfsDAO(HdfsDAO.config());
        hdfs.rmr(inPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(localFile, inPath);
        hdfs.ls(inPath);
        hdfs.cat(inFile);

        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        args = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        job.setConf(HdfsDAO.config());
        job.run(args);

        hdfs.cat(outFile);
    }

}
