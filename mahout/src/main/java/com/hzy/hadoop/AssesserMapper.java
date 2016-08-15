package com.hzy.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.ClusterClassificationConfigKeys;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.iterator.DistanceMeasureCluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterator;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class AssesserMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, IntWritable, FloatWritable> {
		private List<Cluster> clusterModels;
		private static final Logger log = LoggerFactory.getLogger(ClusterAssesser.class);
		protected void setup(Context context) throws IOException, InterruptedException {
		    super.setup(context);
		    
		    Configuration conf = context.getConfiguration();
		    String clustersIn = conf.get(ClusterClassificationConfigKeys.CLUSTERS_IN);
		    		    
		    clusterModels = Lists.newArrayList();
		    
		    if (clustersIn != null && !clustersIn.isEmpty()) {
		      Path clustersInPath = new Path(clustersIn);
		      clusterModels = populateClusterModels(clustersInPath, conf);
		      /*ClusteringPolicy policy = ClusterClassifier
		          .readPolicy(finalClustersPath(clustersInPath));
		      clusterClassifier = new ClusterClassifier(clusterModels, policy);
		      */
		    }
		    //clusterId = new IntWritable();*/
		  }
		/**
		   * Populates a list with clusters present in clusters-*-final directory.
		   * 
		   * @param clusterOutputPath
		   *          The output path of the clustering.
		   * @param conf
		   *          The Hadoop Configuration
		   * @return The list of clusters found by the clustering.
		   * @throws IOException
		   */
		  private static List<Cluster> populateClusterModels(Path clustersIn, Configuration conf) throws IOException {
		    List<Cluster> clusterModels = Lists.newArrayList();
		    Path finalClustersPath = finalClustersPath(conf, clustersIn);
		    Iterator<?> it = new SequenceFileDirValueIterator<Writable>(finalClustersPath, PathType.LIST,
		        PathFilters.partFilter(), null, false, conf);
		    while (it.hasNext()) {
		      ClusterWritable next = (ClusterWritable) it.next();
		      Cluster cluster = next.getValue();
		      cluster.configure(conf);
		      clusterModels.add(cluster);
		    }
		    return clusterModels;
		  }
		  private static Path finalClustersPath(Configuration conf, Path clusterOutputPath) throws IOException {
			    FileSystem fileSystem = clusterOutputPath.getFileSystem(conf);
			    FileStatus[] clusterFiles = fileSystem.listStatus(clusterOutputPath, PathFilters.finalPartFilter());
			    log.info("files: {}", clusterOutputPath.toString());
			    return clusterFiles[0].getPath();
			  }
		  protected void map(IntWritable key, WeightedPropertyVectorWritable vw, Context context)
				    throws IOException, InterruptedException {
			  int clusterId=key.get();
			  double cohension,separation=-1,silhouete;
			  Map<Text,Text> props=vw.getProperties();
			  cohension=Float.valueOf(props.get(new Text("distance")).toString());
			  Vector vector = vw.getVector();
			  for ( Cluster centroid : clusterModels) {
				if (centroid.getId()!=clusterId) {
					
					DistanceMeasureCluster distanceMeasureCluster = (DistanceMeasureCluster) centroid;
				    DistanceMeasure distanceMeasure = distanceMeasureCluster.getMeasure();
				    double f = distanceMeasure.distance(centroid.getCenter(), vector);

					if (f<separation || separation<-0.5) separation=f;
				}
			  }
			  //log.info("separation:{},cohension:{}",separation, cohension);
			  silhouete=(separation-cohension)/(separation>cohension?separation:cohension);
			  FloatWritable value=new FloatWritable();
			  value.set((float) silhouete);
			  
			  IntWritable okey=new IntWritable();
			  okey.set(1);
			  context.write(okey, value);
		  }
		  
}
