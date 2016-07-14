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

        String v = "2";

        String inputPath = HDFS + "/mahout/reuters-out";  //输入文件
        String sequenceFilesFromDirectoryOut = HDFS + "/mahout/hdfs/reuters-seq-" + v;  //seq输出，用于向量
        String sparseVectorsFromSequenceFilesOut = HDFS + "/mahout/hdfs/reuters-vectors-bigram-" + v;  //向量输出，用于聚类输入
        String clusterInputPath = HDFS +  "/mahout/reuters-sparse/tfidf-vectors";
        String clusterPath = HDFS + "/mahout/hdfs/mix_data" + v;
//        String seqFile = inPath + "/seqfile";
//        String seeds = inPath + "/seeds";
        String outPath = clusterPath + "/result/";
        String clusteredPoints = outPath + "clusteredPoints";
        String clusters = "clusters-*-final";
        String clustersPath = HDFS + "/mahout/hdfs/" + clusters;
        Configuration conf = HdfsDAO.config();
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(clusterPath);
        hdfs.mkdirs(clusterPath);
        //hdfs.copyFile(localFile, inPath);
        //hdfs.ls(ClusterOutPath);

        String in = "";
        String out = "";
        String select_value = "";
        String outcluster = "";

        //        mahout seqdirectory -i /mahout/hdfs/input/ -o /mahout/hdfs/reuters-seq-4 -c UTF-8 -xm mapreduce
//        mahout seqdirectory -i file://$(pwd)/reuters-out/ -o file://$(pwd)/reuters-seq-5/ -c UTF-8 -xm sequential
        //java -cp mahout-1.0-SNAPSHOT.jar com.hzy.hadoop.KmeansHadoop2 /mahout/hdfs/input/ /mahout/hdfs/reuters-seq-6 /mahout/hdfs/reuters-vectors-bigram-6-1
        //SequenceFilesFromDirectory 实现将某个文件目录下的所有文件写入一个 SequenceFiles 的功能
        // 它其实本身是一个工具类，可以直接用命令行调用，这里直接调用了它的 main 方法
        //String[] sffd = {"-c", "UTF-8", "-i", args[0], "-o", args[1],"-xm","mapreduce","-ow"};
        String[] sffd = {"-c", "UTF-8", "-i", inputPath, "-o", sequenceFilesFromDirectoryOut,"-xm","mapreduce","-ow"};
        // 解释一下参数的意义：
        // 	 -c: 指定文件的编码形式，这里用的是"UTF-8"
        // 	 -i: 指定输入的文件目录，这里指到我们刚刚导出文件的目录
        // 	 -o: 指定输出的文件目录

        try {
            SequenceFilesFromDirectory.main(sffd);
        } catch (Exception e) {
            e.printStackTrace();
        }




        SparseVectorsFromSequenceFiles sparseVectorsFromSequenceFiles = new SparseVectorsFromSequenceFiles();
//java -cp mahout-1.0-SNAPSHOT.jar com.hzy.hadoop.KmeansHadoop2 /mahout/hdfs/input/ /mahout/hdfs/reuters-seq-6 /mahout/hdfs/reuters-vectors-bigram-6-1

//        mahout seq2sparse -i /mahout/hdfs/reuters-seq-4  -o /mahout/hdfs/reuters-vectors-4 -lnorm -nv -wt tfidf -ow
        //mahout seq2sparse -i /mahout/hdfs/reuters-seq-2 -o /mahout/hdfs/reuters-sparse-2-1 -ow --weight tfidf --maxDFPercent 85 --namedVector
        //SparseVectorsFromSequenceFiles 实现将 SequenceFiles 中的数据进行向量化。
        // 它其实本身是一个工具类，可以直接用命令行调用，这里直接调用了它的 main 方法
        //  "-chunk", "200", "-a","org.apache.lucene.analysis.WhitespaceAnalyzer","-s", "5","-md", "3","-ng", "2",  "-ml", "50"
        String[] svff = {"-i", sequenceFilesFromDirectoryOut, "-o",sparseVectorsFromSequenceFilesOut,"-ow",
                //String[] svff = {"-i", "hdfs://master:8020/mahout/hdfs/reuters-seq-1", "-o","hdfs://master:8020/mahout/hdfs/reuters-vectors-bigram-1","-ow",
                "-wt", "tfidf",
                "-x", "90","--namedVector"};
        //mahout seq2sparse -i /mahout/hdfs/reuters-seq-1 -o /mahout/hdfs/reuters-sparse-1 -ow --weight tfidf --maxDFPercent 85 --namedVector

        // 解释一下参数的意义：
        // 	 -i: 指定输入的文件目录，这里指到我们刚刚生成 SequenceFiles 的目录
        // 	 -o: 指定输出的文件目录
        // 	 -a: 指定使用的 Analyzer，这里用的是 lucene 的空格分词的 Analyzer
        // 	 -chunk: 指定 Chunk 的大小，单位是 M。对于大的文件集合，我们不能一次 load 所有文件，所以需要
        // 		对数据进行切块
        // 	 -wt: 指定分析时采用的计算权重的模式，这里选了 tfidf，其他可选的有tf (当LDA时建议使用)。
        // 	 -s:  指定词语在整个文本集合出现的最低频度，低于这个频度的词汇将被丢掉
        // 	 -md: 指定词语在多少不同的文本中出现的最低值，低于这个值的词汇将被丢掉
        // 	 -x:  指定高频词汇和无意义词汇（例如 is，a，the 等）的出现频率上限，高于上限的将被丢掉
        // 	 -ng: 指定分词后考虑词汇的最大长度，例如 1-gram 就是，coca，cola，这是两个词，
        // 	      2-gram 时，coca cola 是一个词汇，2-gram 比 1-gram 在一定情况下分析的更准确。
        // 	 -ml: 指定判断相邻词语是不是属于一个词汇的相似度阈值，当选择 >1-gram 时才有用，其实计算的是
        // 	      Minimum Log Likelihood Ratio 的阈值
        // 	 -seq: 指定生成的向量是 SequentialAccessSparseVectors，没设置时默认生成还是
        //       RandomAccessSparseVectors
        //   –namedVector (或-nv)：向量会输出附加信息。

        SparseVectorsFromSequenceFiles.main(svff);



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
        String[] parKM = {clusterInputPath,outPath,clustersPath,k,convergenceDelta,maxIter,clustering,distanceMeasure};
        kMeansClusterUsingMapReduce.setArgs(parKM);
        kMeansClusterUsingMapReduce.run();

//
        Path outGlobPath = new Path(outPath, clusters);
        Path clusteredPointsPath = new Path(clusteredPoints);
        System.out.println("outGlobPath=  " + outGlobPath + "  clusteredPointsPath= " + clusteredPointsPath);



        //in = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusters-3-final";
        //outcluster = "/home/hzy/tmp/mahout/out/cluster2.dat";
        outcluster = outPath + "cluster"+v+".dat";
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
        out = outPath + "seq"+ v +".dat";
        String sp = "\n";
        String[] parSFD = {clusteredPoints,out,sp};
        readSeq.setArgs(parSFD);
        readSeq.runJob();

    }
}