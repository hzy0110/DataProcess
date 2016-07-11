package com.hzy.hadoop;

import com.hzy.util.HUtils;
import com.hzy.util.InputDriver;
import com.hzy.util.PropertiesUtil;
import com.hzy.util.Utils;
import org.apache.hadoop.util.ToolRunner;
/**
 * Created by zy on 2016/7/12.
 */
public class InputDriverRunnable implements RunnableWithArgs{
    private String input;
    private String output;
    private String select_value;
    @Override
    public void run() {

//        String[] args=new String[6];
//
//        args[0]="--input";
//        args[1]=input;
//        args[2]="--output";
//        args[3]=output;
//        args[4]="--vector";
//        args[5]=select_value;


        String[] args=new String[]{
                input,
                output,
                select_value
        };

        Utils.printStringArr(args);
        try {
            HUtils.delete(output);
 //           ToolRunner.run(HUtils.getConf()	,new InputDriver()	, args);
			InputDriver.main(args);
        } catch (Exception e) {
            e.printStackTrace();
            // 任务中，报错，需要在任务监控界面体现出来
            HUtils.setRUNNINGJOBERROR(true);
            Utils.simpleLog("InputDriver任务错误！");
        }
    }

    @Override
    public void setArgs(String[] args) {
        this.input=args[0];
        this.output=args[1];
        this.select_value=args[2];
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

    public static void main(String args[]) {
        String HDFS = PropertiesUtil.getValue("hdfs");
        InputDriverRunnable inputDriverRunnable = new InputDriverRunnable();
        String in = "/home/hzy/tmp/mahout/randomData.csv";
        //String in = "/home/hzy/tmp/mahout/reuters-out/reut2-017.sgm-987.txt";
        //String in = "H:/testdata/randomData.csv";
        String out = HDFS + "/mahout/inputdeiveout";
        String select_value = "org.apache.mahout.math.RandomAccessSparseVector";
        String[] s = {in, out, select_value};
        inputDriverRunnable.setArgs(s);
        inputDriverRunnable.run();
    }
}
