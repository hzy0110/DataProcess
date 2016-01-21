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
/************************ 以下代码片段可被所有App共用 ****************************//*


        // 设置App访问Spark使用的用户名：ARCH
        System.setProperty("user.name", "ARCH");

        // 设置App访问Hadoop使用的用户名：ARCH
        System.setProperty("HADOOP_USER_NAME", "ARCH");

        // 在将要传递给Executor的环境中设置Executor访问Hadoop使用的用户名：ARCH
        Map<String, String> envs = new HashMap<String, String>();
        envs.put("HADOOP_USER_NAME", "ARCH");

        // 为App的每个Executor配置最多可以使用的内存量：2GB
        System.setProperty("spark.executor.memory", "2g");

        // 为App的所有Executor配置共计最多可以使用的Core数量（最大并行任务数）：20
        System.setProperty("spark.cores.max", "20");

        // 获取要分发到集群各结点的Jar文件
        // 此例策略：若指定路径为文件，则返回该文件；若指定路径为目录，则列举目录下所有文件
        String[] jars = getApplicationLibrary();

        // 获取Spark上下文对象——访问Spark的起点。构造方法各参数的意义分别为：
        // 1 Spark Master结点的地址；2 App的名称；
        // 3 Spark各Worker结点的Spark部署目录，各结点相同；4 待分发到集群各结点的Jar文件；
        // 5 待传递给Executor环境（仅Map中的部分Key有效）
        JavaSparkContext context = new JavaSparkContext(SPARK_MASTER_ADDRESS,
                "Spark App 0", SPARK_HOME, jars, envs);

        */
/************************ 以上代码片段可被所有App共用 ****************************//*


        // Spark上的词频统计
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
