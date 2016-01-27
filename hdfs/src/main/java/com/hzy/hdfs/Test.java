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
import java.util.ArrayList;
import java.util.List;
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
        Job job = Job.getInstance(conf, "max price");
        job.setJarByClass(Test.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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

    //负责根据名称作为Key，吧价格集中
    public static class Map extends Mapper<Object,Text,Text,IntWritable> {
        private static Text line=new Text();//每行数据

        public void map(Object key,Text value,Context context) throws IOException,InterruptedException {
            line = value;
            //获取名称
            String name = line.toString().split(" ")[0];
            //获取价格
            Integer price = Integer.parseInt(line.toString().split(" ")[1]);
            //System.out.println(price);
            //context.write(line, new Text(""));
            context.write(new Text(name),new IntWritable(price));

        }
    }

    //求最大价格
    public static class Reduce extends Reducer<Text,IntWritable,Text,List<FloatWritable>> {
        //实现reduce函数
        public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
            float maxPrice = Integer.MIN_VALUE;
            float mixPrice = Integer.MAX_VALUE;
            float avgPrice = 0;
            int total = 1;
            List<FloatWritable> listPrice = new ArrayList<>();
            for(IntWritable value:values){
                maxPrice = Math.max(maxPrice, value.get());
                mixPrice = Math.min(mixPrice, value.get());
                avgPrice = avgPrice + value.get();
                total++;
            }
            avgPrice = avgPrice / total;
            listPrice.add(new FloatWritable(maxPrice));
            listPrice.add(new FloatWritable(mixPrice));
            listPrice.add(new FloatWritable(avgPrice));
            context.write(key, listPrice);
        }

    }

}
