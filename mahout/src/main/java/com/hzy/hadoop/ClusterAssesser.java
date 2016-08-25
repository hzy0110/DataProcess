package com.hzy.hadoop;

import java.io.IOException;

import com.hzy.util.HUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.ClusterClassificationConfigKeys;
import org.apache.mahout.clustering.classify.ClusterClassificationDriver;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterAssesser extends AbstractJob {

	private ClusterAssesser() {
		setConf(HUtils.getConf());
	}
	public int run(String[] args) throws Exception {
		addInputOption();
	    addOutputOption();
	    //addOption(DefaultOptionCreator.methodOption().create());
	    addOption(DefaultOptionCreator.clustersInOption()
	        .withDescription("The input centroids, as Vectors.  Must be a SequenceFile of Writable, Cluster/Canopy.")
	        .create());
	    
	    if (parseArguments(args) == null) {
	      return -1;
	    }
	    Path input = getInputPath();
	    Path output = getOutputPath();
	    Path clustersIn = new Path(getOption(DefaultOptionCreator.CLUSTERS_IN_OPTION));
	    if (getConf() == null) {
	      setConf(new Configuration());
	    }
	    run(getConf(), input, clustersIn, output);
		return 0;
	}
	private void run(Configuration conf, Path input, Path clustersIn,
			Path output)throws IOException, InterruptedException,
		      ClassNotFoundException {
		conf.set(ClusterClassificationConfigKeys.CLUSTERS_IN, clustersIn.toUri().toString());
	    
	    Job job = new Job(conf, "Cluster Assesser using silhouete over input: " + input);
	    job.setJarByClass(ClusterAssesser.class);
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setMapperClass(AssesserMapper.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(FloatWritable.class);
	    
	    job.setReducerClass(AssesserReducer.class);
	    job.setNumReduceTasks(1);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    if (!job.waitForCompletion(true)) {
	      throw new InterruptedException("Cluster Classification Driver Job failed processing " + input);
	    }
		
	}
	private static final Logger log = LoggerFactory.getLogger(ClusterAssesser.class);
	  
	  public static void main(String[] args) throws Exception {
	    ToolRunner.run(new Configuration(), new ClusterAssesser(), args);
	  }
	  
}
