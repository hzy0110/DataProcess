package com.hzy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Hzy on 2016/2/4.
 */
public class HdfsImporterHBaseTest  extends Configured implements Tool {

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

    public class Hdfs2HBaseMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException {
            String lineStr = line.toString();
            int index = lineStr.indexOf(":");
            String rowkey = lineStr.substring(0, index);
            String left = lineStr.substring(index+1);
            context.write(new Text(rowkey), new Text(left));
        }
    }


    public class Hdfs2HBaseReducer extends Reducer<Text, Text, ImmutableBytesWritable, Put> {
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
    }

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

        Job job = new Job(conf, "hdfs2hbase");
        job.setJarByClass(HdfsImporterHBaseTest.class);
        job.setMapperClass(Hdfs2HBaseMapper.class);
        job.setReducerClass(Hdfs2HBaseReducer.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, otherArgs[1]);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
