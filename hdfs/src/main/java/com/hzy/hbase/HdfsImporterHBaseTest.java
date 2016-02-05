package com.hzy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Hzy on 2016/2/4.
 */
public class HdfsImporterHBaseTest {




    /*static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {
            System.out.println("进入map");
            String k = key.toString();
            System.out.println("k="+k);
            Put p = new Put(k.getBytes());
            p.add("test1".getBytes(),
                    "test2".getBytes(),
                    Bytes.toBytes(10));
            context.write(null, p);
        }
    }*/

    public static class Hdfs2HBaseMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException {
            String lineStr = line.toString();
            System.out.println("lineStr="+lineStr);
            int index = lineStr.indexOf(":");
            String rowkey = lineStr.substring(0, index);
            String left = lineStr.substring(index+1);
            context.write(new Text(rowkey), new Text(left));
        }
    }


    public static class Hdfs2HBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text rowkey, Iterable<Text> value, Context context) throws IOException,InterruptedException {
            String k = rowkey.toString();
            for(Text val : value) {
                System.out.println("val="+val.toString());
                Put put = new Put(k.getBytes());
                String[] strs = val.toString().split(":");
                String family = strs[0];
                String qualifier = strs[1];
                String v = strs[2];
                put.add(family.getBytes(), qualifier.getBytes(), v.getBytes());
                context.write(new ImmutableBytesWritable(k.getBytes()), put);
            }
        }
    }
/*
    @Override
    public int run(String[] args) throws Exception {
        System.out.println("进入run");
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureImporter <input>");
            return -1;
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "hdfs2hbase");
        job.setJarByClass(HdfsImporterHBaseTest.class);

        job.setMapperClass(Hdfs2HBaseMapper.class);
        job.setReducerClass(Hdfs2HBaseReducer.class);

        job.setMapOutputKeyClass(Text.class);    // +
        job.setMapOutputValueClass(Text.class);  // +

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        //job.setNumReduceTasks(0);
        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "observations");
        return job.waitForCompletion(true) ? 0 : 1;
    }*/

/*    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HdfsImporterHBaseTest(), args);
        System.exit(exitCode);
    }*/

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Usage: wordcount <infile> <table>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "hdfs2hbase");

        job.setJarByClass(HdfsImporterHBaseTest.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setMapperClass(Hdfs2HBaseMapper.class);
        job.setReducerClass(Hdfs2HBaseReducer.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob("observations", Hdfs2HBaseReducer.class, job);



/*      查看了HBASE的源代码中TableOutputFormat和TableMapReduceUtil两个类，
        最终发现TableMapReduceUtil在调用initTableReducerJob初始化时，
        就会调用TableOutputFormat作为输出。唯一不同的是，在初始化的最后，initTableReducerJob函数中会调用initCredentials，
        而这个函数会调用addTokenForJob
        （Checks for an authentication token for the given user, obtaining a new token if necessary,
        and adds it to the credentials for the given map reduce job）。
        */

/*
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

*/
        //job.setOutputFormatClass(TableOutputFormat.class);
        //job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, otherArgs[1]);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
