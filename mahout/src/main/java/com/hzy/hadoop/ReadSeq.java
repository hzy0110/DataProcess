/**
 * 
 */
package com.hzy.hadoop;

import com.hzy.util.Utils;

import java.util.HashMap;
import java.util.Map;
//import org.apache.mahout.utils.SequenceFileDumper;
/**
 * 读取HDFS seq文件
 * @author fansy
 * @date 2015年8月10日
 */
public class ReadSeq implements INotMRJob {
	private String input;
	private String output;
	private String lines;


	@Override
	public void setArgs(String[] args) {
		this.input=args[0];
		this.output=args[1];
		this.lines=args[2];
	}

	@Override
	public Map<String, Object> runJob() {
		Map<String ,Object> map = new HashMap<String,Object>();
		String txt =null;
		map.put("return_show", "readseq_return");
		try{
			String[] args=new String[]{
					"-i",input,
					"-o",output,
					"-n",lines,
					"-sp","<br>"
			};
			Utils.printStringArr(args);
			SequenceFileDumper sf = new SequenceFileDumper();
			sf.run(args);
			txt = sf.getRetStr();
			txt ="序列文件的信息是:<br>"+txt;
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
//		String in = "hdfs://master:8020/mahout/hdfs/mix_data/result/clusteredPoints";
//		String output = "hdfs://master:8020/mahout/reuters-kmeans-seqdumper-3";
//		//String output = "./reuters-kmeans-seqdumper3";
//		String lines = "10";
//		String[] s = {args[0],args[1],args[2]};
//		ReadSeq readSeq =new ReadSeq();
//		readSeq.setArgs(s);
//		readSeq.runJob();


		//		  seqdump 等有序列化文件产生后，测试
		String[] arg= {
				"-i","hdfs://master:8020/mahout/clusteredPoints/part-m-00000",
				"-o","d:/clusters.dat",
				"-n","10",
//				"--help"

		};
//		TestHUtils.getFs().delete(new Path("temp"), true);
		SequenceFileDumper.main(arg);

	}

}
