/**
 * 
 */
package com.hzy.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.util.ReflectionUtils;



/**
 * Hadoop 工具类
 * 
 * @author fansy
 * @date 2015-5-28
 */
public class HUtils {

	
	private static boolean RUNNINGJOBERROR = false;// 运行任务中是否出现错误
	private static boolean ALLJOBSFINISHED=false; // 循环MR任务所有任务成功标志位；
	
	static final String DOWNLOAD_EXTENSION = ".dat";
	private static Configuration conf = null;
	private static FileSystem fs = null;

	public static boolean flag = true; // get configuration from db or file
										// ,true : db,false:file

	public static int JOBNUM = 1; // 一组job的个数
	// 第一个job的启动时间阈值，大于此时间才是要取得的的真正监控任务

	private static long jobStartTime = 0L;// 使用 System.currentTimeMillis() 获得
	private static JobClient jobClient = null;
	
	public static final String HDFSPRE= "/user/root";
	public static final String LOCALPRE= "WEB-INF/classes/data";

	public static Configuration getConf() {

		if (conf == null) {
			conf = new Configuration();
			// get configuration from db or file
			conf.setBoolean("mapreduce.app-submission.cross-platform", "true"
					.equals(PropertiesUtil.getValue("mapreduce.app-submission.cross-platform")));// 配置使用跨平台提交任务
			conf.set("fs.defaultFS", PropertiesUtil.getValue("fs.defaultFS"));// 指定namenode
			conf.set("mapreduce.framework.name",
					PropertiesUtil.getValue("mapreduce.framework.name")); // 指定使用yarn框架
			conf.set("yarn.resourcemanager.address",
					PropertiesUtil.getValue("yarn.resourcemanager.address")); // 指定resourcemanager
			conf.set("yarn.resourcemanager.scheduler.address", PropertiesUtil.getValue(
					"yarn.resourcemanager.scheduler.address"));// 指定资源分配器
			conf.set("mapreduce.jobhistory.address",
					PropertiesUtil.getValue("mapreduce.jobhistory.address"));
		}

		return conf;
	}

	public static FileSystem getFs() {
		if (fs == null) {
			try {
				fs = FileSystem.get(getConf());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fs;
	}

	/**
	 * 获取hdfs文件目录及其子文件夹信息
	 * 
	 * @param input
	 * @param recursive
	 * @return
	 * @throws IOException
	 */
	public static String getHdfsFiles(String input, boolean recursive)
			throws IOException {
		RemoteIterator<LocatedFileStatus> files = getFs().listFiles(
				new Path(input), recursive);
		StringBuffer buff = new StringBuffer();
		while (files.hasNext()) {
			buff.append(files.next().getPath().toString()).append("<br>");
		}

		return buff.toString();
	}

	/**
	 * 根据时间来判断，然后获得Job的状态，以此来进行监控 Job的启动时间和使用system.currentTimeMillis获得的时间是一致的，
	 * 不存在时区不同的问题；
	 * 
	 * @return
	 * @throws IOException
	 */
//	public static List<CurrentJobInfo> getJobs() throws IOException {
//		JobStatus[] jss = getJobClient().getAllJobs();
//		List<CurrentJobInfo> jsList = new ArrayList<CurrentJobInfo>();
//		jsList.clear();
//		for (JobStatus js : jss) {
//			if (js.getStartTime() > jobStartTime) {
//				jsList.add(new CurrentJobInfo(getJobClient().getJob(
//						js.getJobID()), js.getStartTime(), js.getRunState()));
//			}
//		}
//		Collections.sort(jsList);
//		return jsList;
//	}

	public static void printJobStatus(JobStatus js) {
		System.out.println(new java.util.Date() + ":jobId:"
				+ js.getJobID().toString() + ",map:" + js.getMapProgress()
				+ ",reduce:" + js.getReduceProgress() + ",finish:"
				+ js.getRunState());
	}

	/**
	 * @return the jobClient
	 */
	public static JobClient getJobClient() {
		if (jobClient == null) {
			try {
				jobClient = new JobClient(getConf());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return jobClient;
	}

	/**
	 * @param jobClient
	 *            the jobClient to set
	 */
	public static void setJobClient(JobClient jobClient) {
		HUtils.jobClient = jobClient;
	}

	public static long getJobStartTime() {
		return jobStartTime;
	}

	public static void setJobStartTime(long jobStartTime) {
		HUtils.jobStartTime = jobStartTime;
	}

	/**
	 * 判断一组MR任务是否完成
	 * 
	 * @param currentJobInfo
	 * @return
	 */
//	public static String hasFinished(CurrentJobInfo currentJobInfo) {
//
//		if (currentJobInfo != null) {
//			if ("SUCCEEDED".equals(currentJobInfo.getRunState())) {
//				return "success";
//			}
//			if ("FAILED".equals(currentJobInfo.getRunState())) {
//				return "fail";
//			}
//			if ("KILLED".equals(currentJobInfo.getRunState())) {
//				return "kill";
//			}
//		}
//
//		return "running";
//	}

	/**
	 * 返回HDFS路径
	 * 
	 * @param url
	 * @return fs.defaultFs+url
	 */
	public static Path getPath(String url) {

		return new Path(url);
	}

	/**
	 * 获得hdfs全路径
	 * @param url
	 * @return
	 */
	public static String getFullHDFS(String url){
		return PropertiesUtil.getValue("fs.defaultFS") + url;
	}
	/**
	 * 上传本地文件到HFDS
	 * 如果hdfs文件存在则覆盖
	 * 
	 * @param localPath
	 * @param hdfsPath
	 * @return
	 */
	public static Map<String, Object> upload(String localPath, String hdfsPath) {
		Map<String, Object> ret = new HashMap<String, Object>();
		FileSystem fs = getFs();
		Path src = new Path(localPath);
		Path dst = new Path(hdfsPath);
		ret.put("return_show", "upload_return");
		try {
			fs.copyFromLocalFile(src, dst);
			ret.put("return_txt", localPath+"上传至"+hdfsPath+"成功");
			Utils.simpleLog(localPath+"上传至"+hdfsPath+"成功");
		} catch (Exception e) {
			ret.put("flag", "false");
			ret.put("msg", e.getMessage());
			e.printStackTrace();
			return ret;
		}
		ret.put("flag", "true");
		ret.put("msg", "HFDS:'"+hdfsPath+"'");
		return ret;
	}

	/**
	 * 删除HFDS文件或者文件夹
	 * 
	 * @param hdfsFolder
	 * @return
	 */
	public static boolean delete(String hdfsFolder) {
		try {
			getFs().delete(getPath(hdfsFolder), true);
		} catch (IOException e) {
			
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 下载文件
	 * @param hdfsPath
	 * @param localPath
	 *            ,本地文件夹
	 * @return
	 */
	public static Map<String, Object> downLoad(String hdfsPath, String localPath) {
		Map<String, Object> ret = new HashMap<String, Object>();
		FileSystem fs = getFs();
		Path src = new Path(hdfsPath);
		Path dst = new Path(localPath);
		try {
			RemoteIterator<LocatedFileStatus> fss = fs.listFiles(src, true);
			int i = 0;
			while (fss.hasNext()) {
				LocatedFileStatus file = fss.next();
				if (file.isFile() && file.toString().contains("part")) {
					// 使用这个才能下载成功
					fs.copyToLocalFile(false, file.getPath(), new Path(dst,
							"hdfs_" + (i++) + HUtils.DOWNLOAD_EXTENSION), true);
				}
			}
		} catch (Exception e) {
			ret.put("flag", "false");
			ret.put("msg", e.getMessage());
			e.printStackTrace();
			return ret;
		}
		ret.put("flag", "true");
		return ret;
	}

	/**
	 * 移动文件
	 * 
	 * @param hdfsPath
	 * @param desHdfsPath
	 * @return
	 */
	public static boolean mv(String hdfsPath, String desHdfsPath) {

		return false;
	}

	/**
	 * 读取HDFS文件并写入本地
	 * 
	 * @param url
	 * @param localPath
	 */
	public static void readHDFSFile(String url, String localPath) {
		Path path = new Path(url);
		// Configuration conf = HUtils.getConf();
		FileWriter writer = null;
		BufferedWriter bw = null;
		InputStream in = null;
		try {
			writer = new FileWriter(localPath);
			bw = new BufferedWriter(writer);
			// FileSystem fs = FileSystem.get(URI.create(url), conf);
			FileSystem fs = getFs();
			in = fs.open(path);
			BufferedReader read = new BufferedReader(new InputStreamReader(in));
			String line = null;

			while ((line = read.readLine()) != null) {
				// System.out.println("result:"+line.trim());
				// [5.5,4.2,1.4,0.2] 5,0.3464101615137755
				String[] lines = line.split("\t");
				bw.write(lines[1]);
				bw.newLine();
			}
			System.out.println(new java.util.Date() + "ds file:" + localPath);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				bw.close();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	
	/**
	 * double数组转为字符串
	 * 
	 * @param d
	 * @return
	 */
	public static String doubleArr2Str(double[] d) {
		StringBuffer buff = new StringBuffer();

		for (int i = 0; i < d.length; i++) {
			buff.append(d[i]).append(",");
		}
		return buff.substring(0, buff.length() - 1);
	}

	/**
	 * int 数组转为字符串
	 * 
	 * @param d
	 * @return
	 */
	public static String intArr2Str(int[] d) {
		StringBuffer buff = new StringBuffer();

		for (int i = 0; i < d.length; i++) {
			buff.append(d[i]).append(",");
		}
		return buff.substring(0, buff.length() - 1);
	}

	/**
	 * 如果jsonMap.get("rows")的List的size不够JOBNUM，那么就需要添加
	 * 
	 * @param jsonMap
	 */
//	public static void checkJsonMap(Map<String, Object> jsonMap) {
//		@SuppressWarnings("unchecked")
//		List<CurrentJobInfo> list = (List<CurrentJobInfo>) jsonMap.get("rows");
//		if (list.size() == HUtils.JOBNUM) {
//			return;
//		}
//		for (int i = list.size(); i < HUtils.JOBNUM; i++) {
//			list.add(new CurrentJobInfo());
//		}
//
//	}
	/**
	 * 按照行数读取txt文件，并返回字符串
	 * @param input
	 * @param lines
	 * @param splitter : 每行数据的分隔符
	 * @return
	 * @throws Exception 
	 */
	public static String readTxt(String input,String lines,String splitter) throws Exception{
		StringBuffer buff = new StringBuffer();
		Path path = new Path(input);
		long lineNum= Long.parseLong(lines);
		InputStream in = null;
		try {
			FileSystem fs = getFs();
			in = fs.open(path);
			BufferedReader read = new BufferedReader(new InputStreamReader(in));
			String line = null;

			while ((line = read.readLine()) != null&&lineNum-->0) {
				buff.append(line).append(splitter);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw e;
			}
		}
		return buff.toString();
	}

	/**
	 * 读取给定序列文件的前面k条记录
	 * @param input
	 * @param k
	 * @return
	 */
	public static Map<Object, Object> readSeq(String input, int k) {
		Map<Object,Object> map= new HashMap<Object,Object>();
		Path path = HUtils.getPath(input);
		Configuration conf = HUtils.getConf();
		Reader reader = null;
		try {
			reader = new Reader(conf, Reader.file(path),
					Reader.bufferSize(4096), Reader.start(0));
			Writable dkey =  (Writable) ReflectionUtils
					.newInstance(reader.getKeyClass(), conf);
			Writable dvalue =  (Writable) ReflectionUtils
					.newInstance(reader.getValueClass(), conf);

			while (reader.next(dkey, dvalue)&&k>0) {// 循环读取文件
				// 使用这个进行克隆
				map.put(WritableUtils.clone(dkey, conf),WritableUtils.clone( dvalue,conf));
				k--;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return map;
	}

	/**
	 * 
	 * @param input
	 * @param output
	 */
	public static void copy(String input, String output) {
		FileSystem fs =getFs();
		Configuration conf = getConf();
		Path in= new Path(input);
		Path out= new Path(output);
		
		try{
			if(fs.exists(out)){//如果存在则删除
				fs.delete(out, true);
			}
//			fs.create(out);// 新建
			fs.mkdirs(out);
			FileStatus[] files=fs.listStatus(in);
			Path[] srcs= new Path[files.length];
			for(int i=0;i<files.length;i++){
				srcs[i]=files[i].getPath();
			}
			boolean flag =FileUtil.copy(fs,srcs,fs,out,false,true,conf);
			Utils.simpleLog("数据从"+input.toString()+"传输到"+out.toString()+
					(flag?"成功":"失败")+"!");
		}catch(Exception e){
			e.printStackTrace();
			Utils.simpleLog("数据从"+input.toString()+"传输到"+out.toString()+
					"失败"+"!");
		}
	}
	/**
	 * 获得一个文件夹的信息
	 * 用于比较前后两次文件夹的内容是否一致
	 * @param folder
	 * @param flag true则floder字符串包含fs信息，否则不包含
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static String getFolderInfo(String folder,String flag) throws FileNotFoundException, IOException{
		StringBuffer buff = new StringBuffer();
		FileSystem fs = getFs();
		Path path = getPath(folder);
		
		FileStatus[] files = fs.listStatus(path);
		
		buff.append("contain files:"+files.length+"\t[");
		String filename="";
		for(FileStatus file:files){
			path = file.getPath();
			filename=path.toString();
			buff.append(filename.substring(filename.length()-1))
				.append(":").append(file.getLen()).append(",");
		}
		filename=buff.substring(0, buff.length()-1);
		
		
		return filename+"]";
	}
	/**
	 * 获得一个文件夹下面的文件个数
	 * @param folder
	 * @param flag true则floder字符串包含fs信息，否则不包含
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static  int getFolderFilesNum(String folder,String flag) throws FileNotFoundException, IOException{
		FileSystem fs = getFs();
		Path path = getPath(folder);
		
		FileStatus[] files = fs.listStatus(path);
		return files.length;
	}

	/**
	 * 删除_center/iter_i (i>0)的所有文件夹
	 * @param output 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void clearCenter(String output) throws FileNotFoundException, IOException {
		
		FileStatus[] fss = getFs().listStatus(HUtils.getPath(output));
		
		for(FileStatus f:fss){
			if(f.toString().contains("iter_0")){
				continue;
			}
			getFs().delete(f.getPath(), true);
			Utils.simpleLog("删除文件"+f.getPath().toString()+"!");
		}
	}

	/**
	 * @return the rUNNINGJOBERROR
	 */
	public static boolean isRUNNINGJOBERROR() {
		return RUNNINGJOBERROR;
	}

	/**
	 * @param rUNNINGJOBERROR the rUNNINGJOBERROR to set
	 */
	public static void setRUNNINGJOBERROR(boolean rUNNINGJOBERROR) {
		RUNNINGJOBERROR = rUNNINGJOBERROR;
	}

	/**
	 * @return the aLLJOBSFINISHED
	 */
	public static boolean isALLJOBSFINISHED() {
		return ALLJOBSFINISHED;
	}

	/**
	 * @param aLLJOBSFINISHED the aLLJOBSFINISHED to set
	 */
	public static void setALLJOBSFINISHED(boolean aLLJOBSFINISHED) {
		ALLJOBSFINISHED = aLLJOBSFINISHED;
	}


}
