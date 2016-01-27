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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
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

    public static void main(String[] args) throws IOException, Exception {
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

        //reduce阶段的输出的key
        job.setOutputKeyClass(Text.class);
        //reduce阶段的输出的value
        job.setOutputValueClass(IntWritable.class);

        //加入控制容器
        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        Job job2 = Job.getInstance(conf, "order total");
        job2.setJarByClass(Test.class);

        job2.setMapperClass(OrderMap.class);
        job2.setReducerClass(Order.class);

        //reduce阶段的输出的key
        job2.setOutputKeyClass(Text.class);
        //reduce阶段的输出的value
        job2.setOutputValueClass(FloatWritable.class);


        //作业2加入控制容器
        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);

        //设置多个作业直接的依赖关系
        //如下所写：
        //意思为job2的启动，依赖于job1作业的完成
        ctrljob2.addDependingJob(ctrljob1);


        //输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
        FileInputFormat.addInputPath(job2, new Path(args[1]));

        //输出路径从新传入一个参数，这里需要注意，因为我们最后的输出文件一定要是没有出现过得
        //因此我们在这里new Path(args[2])因为args[2]在上面没有用过，只要和上面不同就可以了
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "1"));


        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("myctrl");

        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);

        //在线程启动，记住一定要有这个
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {

            if (jobCtrl.allFinished()) {//如果作业成功完成，就打印成功作业的信息
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

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
            extends Reducer<Text, IntWritable, Text, IntWritable> {
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
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private static Text line = new Text();//每行数据

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            //获取名称
            String name = line.toString().split(" ")[0];
            //获取价格
            Integer price = Integer.parseInt(line.toString().split(" ")[1]);
            //System.out.println(price);
            //context.write(line, new Text(""));
            context.write(new Text(name), new IntWritable(price));

        }
    }

    //求最大价格
    public static class Reduce extends Reducer<Text, IntWritable, Text, List<FloatWritable>> {
        //实现reduce函数
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float maxPrice = Integer.MIN_VALUE;
            float mixPrice = Integer.MAX_VALUE;
            float avgPrice = 0;
            int total = 0;
            List<FloatWritable> listPrice = new ArrayList<>();
            for (IntWritable value : values) {
                maxPrice = Math.max(maxPrice, value.get());
                mixPrice = Math.min(mixPrice, value.get());
                avgPrice += value.get();
                total++;
            }

            avgPrice = avgPrice / total;
            listPrice.add(new FloatWritable(maxPrice));
            listPrice.add(new FloatWritable(mixPrice));
            listPrice.add(new FloatWritable(avgPrice));
            listPrice.add(new FloatWritable(total));
            context.write(key, listPrice);
        }


    }

    //第二次map
    public static class OrderMap extends Mapper<Text,List<FloatWritable>,Text,List<FloatWritable>> {
        private static Text line = new Text();//每行数据

        public void OrderMap(Text key,List<FloatWritable> values,Reducer.Context context) throws IOException, InterruptedException {
            System.out.println("第二次map的key"+key);
            context.write(new Text(key), values);

        }
    }

    //按照单个名字出现次数排序
    public static class Order extends Reducer<Text,List<FloatWritable>,Text,List<FloatWritable>> {
        //实现reduce函数
        public void order(Text key,List<FloatWritable> values,Context context)throws IOException,InterruptedException{
            Collections.sort(values);
            context.write(key, values);
        }

    }
}
