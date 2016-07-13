/**
 * 
 */
package com.hzy.hadoop;

import com.hzy.util.HUtils;
import com.hzy.util.Utils;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

/**
 * 读取HDFS 聚类中心cluster文件
 * @author fansy
 * @date 2015年8月10日
 */
public class ReadCluster implements INotMRJob {
	private String input;
	private String output;
	private String points;
	private String distanceMeasure;
	private String include_per_cluster;


	@Override
	public void setArgs(String[] args) {
		this.input=args[0];
		this.output=args[1];
		this.points=args[2];
		this.distanceMeasure=args[3];
		this.include_per_cluster=args[4];
	}

	@Override
	public Map<String, Object> runJob() {
		Map<String ,Object> map = new HashMap<String,Object>();
		String txt =null;
		map.put("return_show", "readcluster_return");
		try{
			String[] args=null;
			if("-1".equals(include_per_cluster)){
				args = new String[8];
			}else{
				args= new String[10];
				args[8]="-sp";
				args[9]=include_per_cluster;
			}
			args[0]="-i";
			args[1]=input;
			args[2]="-o";
			args[3]=output;
			args[4]="-p";
			args[5]=points;
			args[6]="-dm";
			args[7]=distanceMeasure;
			Utils.printStringArr(args);
			ClusterDumper cd = new ClusterDumper();
			cd.run(args);
			txt= cd.printClusters(null, "<br>");
			txt ="聚类中心及数据是:<br>"+txt;
			map.put("flag", "true");

			map.put("return_txt", txt);
		}catch(Exception e){
			e.printStackTrace();
			map.put("flag", "false");
			map.put("monitor", "false");
			map.put("msg", input+"读取失败！");
		}
		return map;
	}

	public static void main(String[] args) throws Exception {
		String in = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusters-3-final";
		String out = "H:/clusters.dat";
		String points = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints/part-m-00000";
		String distanceMeasure = "org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure";
		String include_per_cluster = "2";
		String[] s = {in,out,points,distanceMeasure,include_per_cluster};
		ReadCluster readCluster =new ReadCluster();
		readCluster.setArgs(s);
		readCluster.runJob();
/*
		//直接执行main成功
		String[] arg= {
				"-i","hdfs://master:8020/mahout/hdfs/mix_data/result/clusters-3-final",
				"-o","H:/clusters.dat",
				"-of","TEXT",
				"-p","hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints/part-m-00000",
				"-sp","2",
//			"-e",
				"-dm","org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure",
				"--tempDir","temp",
//				"--help"
		};
		//HUtils.getFs().delete(new Path("temp"), true);
//		TestHUtils.getFs().delete(new Path("tmp/representative"), true);

		//HUtils.getFs().delete(new Path("/use/root/utils/clusterdumper/output"),true);
		//org.apache.mahout.utils.clustering.ClusterDumper.main(arg);
		ClusterDumper.main(arg);*/
		/*
			直接使用命令行参数成功
		*  -i   hdfs://master:8020/mahout/hdfs/mix_data/result/clusters-3-final   -o   ./clusters1.dat   -of   TEXT   -p   hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints/part-m-00000   -sp   2   -dm   org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure  --tempDir   temp
		*
		*
		* */
	}
}
