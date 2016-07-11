package com.hzy.hadoop;

import com.hzy.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

/**
 * Created by Hzy on 2016/7/11.
 */

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;

import com.hzy.util.HUtils;
import com.hzy.util.Utils;

public class kMeansClusterUsingMapReduce {




    private static final String HDFS = PropertiesUtil.getValue("hdfs");

    private String input;
    private String output;
    private String clusters;
    private String k;
    private String convergenceDelta;
    private String maxIter;
    private String clustering;
    private String  distanceMeasure;
    //@Override
    public void run() {

        String[] args=null;
        if("true".equals(clustering)){
            args=new String[17];
            args[16]="-cl";
        }else{
            args= new String[16];
        }
        args[0]="-i";
        args[1]=input;
        args[2]="-o";
        args[3]=output;
        args[4]="-c";
        args[5]=clusters;
        args[6]="-k";
        args[7]=k;
        args[8]="-cd";
        args[9]=convergenceDelta;
        args[10]="-x";
        args[11]=maxIter;
        args[12]="-dm";
        args[13]=distanceMeasure;
        args[14]="--tempDir";
        args[15]="temp";

        Utils.printStringArr(args);
        try {
            HUtils.delete(output);
            HUtils.delete("temp");
            HUtils.delete(clusters);

            int ret = ToolRunner.run(HUtils.getConf()	,new KMeansDriver()	, args);
            if(ret==0){// 所有任务运行完成
                HUtils.setALLJOBSFINISHED(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 任务中，报错，需要在任务监控界面体现出来
            HUtils.setRUNNINGJOBERROR(true);
            Utils.simpleLog("KMeansDriver任务错误！");
        }
    }

    //@Override
    public void setArgs(String[] args) {
        this.input=args[0];
        this.output=args[1];
        this.clusters=args[2];
        this.k=args[3];
        this.convergenceDelta=args[4];
        this.maxIter=args[5];
        this.clustering=args[6];
        this.distanceMeasure=args[7];

    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }
    public static void main(String[] args) throws Exception {
        String in  = HDFS +"/mahout/reuters-sparse/tfidf-vectors";
        String out = HDFS +"/mahout/kmeans/"+"resule";
        String clusters = "clusters-*-final";
        String k = "3";
        String convergenceDelta = "0.1";
        String maxIter = "100";
        String clustering = "true";
        String distanceMeasure = "org.apache.mahout.common.distance.CosineDistanceMeasure";
        String[] s = {in,out,clusters,k,convergenceDelta,maxIter,clustering,distanceMeasure};
        kMeansClusterUsingMapReduce kMeansClusterUsingMapReduce = new kMeansClusterUsingMapReduce();
        kMeansClusterUsingMapReduce.setArgs(s);
        kMeansClusterUsingMapReduce.run();
    }
}