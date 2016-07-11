package com.hzy.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CityBlockSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CooccurrenceCountSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.LoglikelihoodSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.TanimotoCoefficientSimilarity;
/**
 * Created by Hzy on 2016/7/11.
 */
public class MahoutJobTest {
    public static void main(String args[]) throws Exception{
        Configuration conf= new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.100:9000");
        conf.set("hadoop.job.user", "hadoop");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.jobtracker.address", "192.168.1.101:9001");
        conf.set("yarn.resourcemanager.hostname", "192.168.1.101");
        conf.set("yarn.resourcemanager.admin.address", "192.168.1.101:8033");
        conf.set("yarn.resourcemanager.address", "192.168.1.101:8032");
        conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.1.101:8031");
        conf.set("yarn.resourcemanager.scheduler.address", "192.168.1.101:8030");

        String[] str ={
                "-i","hdfs://192.168.1.100:9000/data/test_in/mahout_in1.csv",
                "-o","hdfs://192.168.1.100:9000/data/test_out/mahout_out_CityBlockSimilarity/rec001",
                "-n","3",
                "-b","false",

                //mahout自带的相似类列表
//                 SIMILARITY_COOCCURRENCE(CooccurrenceCountSimilarity.class),
//                 SIMILARITY_LOGLIKELIHOOD(LoglikelihoodSimilarity.class),
//                 SIMILARITY_TANIMOTO_COEFFICIENT(TanimotoCoefficientSimilarity.class),
//                 SIMILARITY_CITY_BLOCK(CityBlockSimilarity.class),
//                 SIMILARITY_COSINE(CityBlockSimilarity.class),
//                 SIMILARITY_PEARSON_CORRELATION(CosineSimilarity.class),
//                 SIMILARITY_EUCLIDEAN_DISTANCE(EuclideanDistanceSimilarity.class);
                "-s","SIMILARITY_CITY_BLOCK",

                "--maxPrefsPerUser","70",
                "--minPrefsPerUser","2",
                "--maxPrefsInItemSimilarity","70",
                "--outputPathForSimilarityMatrix","hdfs://192.168.1.100:9000/data/test_out/mahout_out_CityBlockSimilarity/matrix/rec001",
                "--tempDir","hdfs://192.168.1.100:9000/data/test_out/mahout_out_CityBlockSimilarity/temp/rec001"
        };

        ToolRunner.run(conf, new RecommenderJob(), str);
    }
}
