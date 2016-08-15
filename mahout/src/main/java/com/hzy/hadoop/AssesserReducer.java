package com.hzy.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssesserReducer extends Reducer<IntWritable, FloatWritable, Text, FloatWritable> {
	private static final Logger log = LoggerFactory.getLogger(ClusterAssesser.class);
	protected void setup(Context context) throws IOException, InterruptedException {
	    super.setup(context);
	    log.info("reducer");
	}
	public void reduce(IntWritable key, Iterable<FloatWritable> values,
			Context context)
			throws IOException, InterruptedException {
			int cnt=0; 
			float total=0;
			for (FloatWritable value : values) {
			total=total+ value.get();
			cnt++;
			}
			log.debug("silhouete:{}",total/cnt);
			context.write(new Text("silhouete"), new FloatWritable(total/cnt));
	}

}
