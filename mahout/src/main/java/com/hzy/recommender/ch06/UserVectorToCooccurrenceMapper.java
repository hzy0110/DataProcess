/*
 * Source code for Listing 6.3
 * 
 */
package com.hzy.recommender.ch06;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class UserVectorToCooccurrenceMapper extends
		Mapper<VarLongWritable, VectorWritable, IntWritable, IntWritable> {

	public void map(VarLongWritable userID, VectorWritable userVector,
			Context context) throws IOException, InterruptedException {
		Iterable<Vector.Element> iterable = userVector.get().nonZeroes();

		Iterator<Vector.Element> it = iterable.iterator();
		while (it.hasNext()) {
			int index1 = it.next().index();
			Iterable<Vector.Element> iterable2 = userVector.get().nonZeroes();
			Iterator<Vector.Element> it2 = iterable2.iterator();
			while (it2.hasNext()) {
				int index2 = it2.next().index();
				context.write(new IntWritable(index1), new IntWritable(index2));
			}
		}
	}
}
