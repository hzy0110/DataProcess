package com.hzy.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by Hzy on 2016/1/15.
 */
public class WordCount {
/*

    static final String SPARK_MASTER_ADDRESS = "spark://hadoop01:7077";
    static final String SPARK_HOME = "/home/ARCH/spark";
    static final String APP_LIB_PATH = "lib";

    public static void main(String[] args) throws Exception {

        */
/************************ ���´���Ƭ�οɱ�����App���� ****************************//*


        // ����App����Sparkʹ�õ��û�����ARCH
        System.setProperty("user.name", "ARCH");

        // ����App����Hadoopʹ�õ��û�����ARCH
        System.setProperty("HADOOP_USER_NAME", "ARCH");

        // �ڽ�Ҫ���ݸ�Executor�Ļ���������Executor����Hadoopʹ�õ��û�����ARCH
        Map<String, String> envs = new HashMap<String, String>();
        envs.put("HADOOP_USER_NAME", "ARCH");

        // ΪApp��ÿ��Executor����������ʹ�õ��ڴ�����2GB
        System.setProperty("spark.executor.memory", "2g");

        // ΪApp������Executor���ù���������ʹ�õ�Core���������������������20
        System.setProperty("spark.cores.max", "20");

        // ��ȡҪ�ַ�����Ⱥ������Jar�ļ�
        // �������ԣ���ָ��·��Ϊ�ļ����򷵻ظ��ļ�����ָ��·��ΪĿ¼�����о�Ŀ¼�������ļ�
        String[] jars = getApplicationLibrary();

        // ��ȡSpark�����Ķ��󡪡�����Spark����㡣���췽��������������ֱ�Ϊ��
        // 1 Spark Master���ĵ�ַ��2 App�����ƣ�
        // 3 Spark��Worker����Spark����Ŀ¼���������ͬ��4 ���ַ�����Ⱥ������Jar�ļ���
        // 5 �����ݸ�Executor��������Map�еĲ���Key��Ч��
        JavaSparkContext context = new JavaSparkContext(SPARK_MASTER_ADDRESS,
                "Spark App 0", SPARK_HOME, jars, envs);

        */
/************************ ���ϴ���Ƭ�οɱ�����App���� ****************************//*


        // Spark�ϵĴ�Ƶͳ��
        countWords(context);

    }

    private static String[] getApplicationLibrary()
            throws IOException {
        List<String> list = new LinkedList<String>();
        File lib = new File(APP_LIB_PATH);
        if (lib.exists()) {
            if (lib.isFile() && lib.getName().endsWith(".jar")) {
                list.add(lib.getCanonicalPath());
            } else {
                for (File file : lib.listFiles()) {
                    if (file.isFile()&& file.getName().endsWith(".jar"))
                        list.add(file.getCanonicalPath());
                }
            }
        }
        String[] ret = new String[list.size()];
        int i = 0;
        for (String s : list)
            ret[i++] = s;
        return ret;
    }

    private static void countWords(JavaSparkContext context)
            throws Exception {
        String input = "hdfs://hadoop01:8020/user/ARCH/a.txt";
        JavaRDD<String> data =   context.textFile(input).cache();
        JavaPairRDD<String, Integer> pairs;
        pairs = data.flatMap(new SplitFunction());
        pairs = pairs.reduceByKey(new ReduceFunction());
        String output =  "hdfs://hadoop01:8020/user/ARCH/output";
        pairs.saveAsTextFile(output);
    }

    private static class SplitFunction extends
            PairFlatMapFunction<String, String, Integer> {
        private static final long serialVersionUID = 41959375063L;

        public Iterable<Tuple2<String, Integer>> call(String line)
                throws Exception {
            List<Tuple2<String, Integer>> list;
            list = new LinkedList<Tuple2<String, Integer>>();
            for (String word : line.split(" "))
                list.add(new Tuple2<String, Integer>(word, 1));
            return list;
        }
    }

    private static class ReduceFunction extends
            Function2<Integer, Integer, Integer> {
        private static final long serialVersionUID = 5446148657508L;

        public Integer call(Integer a, Integer b) throws Exception {
            return a + b;
        }
    }
*/

}
