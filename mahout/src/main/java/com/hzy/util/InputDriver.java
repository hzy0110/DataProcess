
package com.hzy.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.clustering.conversion.InputMapper;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
                               * This class converts text files containing space-delimited floating point numbers into
 * Mahout sequence files of VectorWritable suitable for input to the clustering jobs in
 * particular, and any Mahout job requiring this input in general.
 *
 */
public  class InputDriver extends Configured implements Tool {
  
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
   
    
  }
  
  public static int runJob(Path input, Path output, String vectorClassName,Configuration conf)
    throws IOException, InterruptedException, ClassNotFoundException {
    conf.set("vector.implementation.class.name", vectorClassName);
    Job job = Job.getInstance(conf);
    job.setJobName("Input Driver running over input: " + input);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VectorWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(InputMapper.class);
    job.setNumReduceTasks(0);
    job.setJarByClass(org.apache.mahout.clustering.conversion.InputDriver.class);
    
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new IllegalStateException("Job failed!");
    }
    return succeeded?0:-1;
  }

@Override
public int run(String[] args) throws Exception {
	if(args.length!=3){
		throw new Exception("输入参数不对，应该为3个：<input> <output> <vectorname>");
	}
	Configuration conf = getConf();
	Path input = new Path(args[0]);
	Path output = new Path(args[1]);
	String vectorClassName = args[2];
	return runJob(input, output, vectorClassName,conf);
	
}
  
}
