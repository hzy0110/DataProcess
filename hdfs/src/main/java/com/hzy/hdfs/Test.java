package com.hzy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.hzy.utils.HdfsUtil;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * Created by Hzy on 2016/1/21.
 */
public class Test {
    public static String hdfsUrl = "hdfs://192.168.189.142:8020";
    public static String pathfile = "/tmp/hdfs/test.txt";
    public static String pathfilein = "H:\\java\\test\\in\\testin.txt";
    public static String pathfileout = "H:\\java\\test\\out\\testout.txt";
    public static String path = "/tmp/hdfs";

    public static void main(String[] args) throws IOException,Exception{
        //Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);
        //Path file = new Path(pathfile);
        //Path filein = new Path(pathfilein);
        //Path fileout = new Path(pathfileout);
        //HdfsUtil.readFile(fs, file);
        //HdfsUtil.createWriteFile(fs,file,"test");
        //fs.close();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Test.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static class Map extends Mapper<Object,Text,Text,Text> {
        private static Text line=new Text();//每行数据

        public void map(Object key,Text value,Context context) throws IOException,InterruptedException {
            line = value;
            context.write(line, new Text(""));
        }
    }

    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        //实现reduce函数
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
            context.write(key, new Text(""));
        }

    }

}
