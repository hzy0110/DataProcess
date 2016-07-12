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
	private String points;
	private String distanceMeasure;
	private String include_per_cluster;


	@Override
	public void setArgs(String[] args) {
		this.input=args[0];
		this.points=args[1];
		this.distanceMeasure=args[2];
		this.include_per_cluster=args[3];
	}

	@Override
	public Map<String, Object> runJob() {
		Map<String ,Object> map = new HashMap<String,Object>();
		String txt =null;
		map.put("return_show", "readcluster_return");
		try{
			String[] args=null;
			if("-1".equals(include_per_cluster)){
				args = new String[6];
			}else{
				args= new String[8];
				args[6]="-sp";
				args[7]=include_per_cluster;
			}
			args[0]="-i";
			args[1]=input;
			args[2]="-p";
			args[3]=points;
			args[4]="-dm";
			args[5]=distanceMeasure;
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
//		String in = "";
//		String points = "";
//		String distanceMeasure = "";
//		String include_per_cluster = "";
//		String[] s = {args[0],points,distanceMeasure,include_per_cluster};
//		ReadCluster readCluster =new ReadCluster();
//		readCluster.setArgs(s);
//		readCluster.runJob();

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
		org.apache.mahout.utils.clustering.ClusterDumper.main(arg);

	}

}
