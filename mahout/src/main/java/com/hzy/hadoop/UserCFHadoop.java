package com.hzy.hadoop;

import com.hzy.util.PropertiesUtil;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

/**
 * Created by Hzy on 2016/7/11.
 */
public class UserCFHadoop {
    private static final String HDFS = PropertiesUtil.getValue("hdfs");
    public static void main(String[] args) throws Exception {
        String localFile = "/home/hzy/tmp/mahout/item.csv";
        //String localFile = "H:/testdata/item.csv";
        String inPath = HDFS + "/mahout/itemCF";
        String inFile = inPath + "/item.csv";
        String outPath = HDFS + "/mahout/itemCF/result/";
        String outFile = outPath + "/part-r-00000";
        String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();
        //String tmpPath = "h:/tmp/" + System.currentTimeMillis();


        HdfsDAO hdfs = new HdfsDAO(HdfsDAO.config());
        hdfs.rmr(inPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(localFile, inPath);
        hdfs.ls(inPath);
        hdfs.cat(inFile);

        RecommenderJob job = new RecommenderJob();
        job.setConf(HdfsDAO.config());
        job.run(args);

        hdfs.cat(outFile);



    }

}
