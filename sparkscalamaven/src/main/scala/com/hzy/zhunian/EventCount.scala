package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计算每场人数
  * Created by Hzy on 2016/2/17.
  */
object EventCount {
  def filename: String = "zhunian_EventCount_";

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_EventCount");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");


    //计算每行人数
    val znpCount = textFile.map(line => (line.split(":")(0),line.split("-"))).
      filter(line =>line._2.length > 1).
      map(line => (line._1,line._2(1).split(",").length)).
      reduceByKey((a, b) => a + b).sortBy(_._2, false)

    znpCount.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");


  }

}
