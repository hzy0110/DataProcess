package com.hzy.spark;

import org.apache.spark.SparkConf;

/**
 * Created by zy on 2016/1/16.
 */
public class WorkCount4 {

    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");

}
