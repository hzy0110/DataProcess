package com.hzy.hadoop;

/**
 * Created by Hzy on 2016/4/2.
 */

import com.hzy.util.PropertiesUtil;
import com.hzy.util.SparseVectorsFromSequenceFiles;

public class KmeansHadoop3 {
    private static final String HDFS = PropertiesUtil.getValue("hdfs");

    public static void main(String[] args) throws Exception {
//        mahout seqdirectory -i /mahout/hdfs/input/ -o /mahout/hdfs/reuters-seq-4 -c UTF-8
//        mahout seqdirectory -i file://$(pwd)/reuters-out/ -o file://$(pwd)/reuters-seq-5/ -c UTF-8 -xm sequential
        //SequenceFilesFromDirectory 实现将某个文件目录下的所有文件写入一个 SequenceFiles 的功能
        // 它其实本身是一个工具类，可以直接用命令行调用，这里直接调用了它的 main 方法
        String[] sffd = {"-c", "UTF-8", "-i", args[0], "-o", args[1]};
        // 解释一下参数的意义：
        // 	 -c: 指定文件的编码形式，这里用的是"UTF-8"
        // 	 -i: 指定输入的文件目录，这里指到我们刚刚导出文件的目录
        // 	 -o: 指定输出的文件目录

        try {
            //SequenceFilesFromDirectory.main(sffd);
        } catch (Exception e) {
            e.printStackTrace();
        }




    SparseVectorsFromSequenceFiles sparseVectorsFromSequenceFiles = new SparseVectorsFromSequenceFiles();
//java -cp mahout-1.0-SNAPSHOT.jar com.hzy.hadoop.KmeansHadoop2 hdfs://master:8020/mahout/hdfs/reuters-seq-1 hdfs://master:8020/mahout/hdfs/reuters-vectors-bigram-1

//        mahout seq2sparse -i /mahout/hdfs/reuters-seq-4  -o /mahout/hdfs/reuters-vectors-4 -lnorm -nv -wt tfidf -ow
        //mahout seq2sparse -i /mahout/hdfs/reuters-seq-2 -o /mahout/hdfs/reuters-sparse-2-1 -ow --weight tfidf --maxDFPercent 85 --namedVector
        //SparseVectorsFromSequenceFiles 实现将 SequenceFiles 中的数据进行向量化。
        // 它其实本身是一个工具类，可以直接用命令行调用，这里直接调用了它的 main 方法
        //  "-chunk", "200", "-a","org.apache.lucene.analysis.WhitespaceAnalyzer","-s", "5","-md", "3","-ng", "2",  "-ml", "50"
        String[] svff = {"-i", args[1], "-o",args[2],"-ow",
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
    }
}