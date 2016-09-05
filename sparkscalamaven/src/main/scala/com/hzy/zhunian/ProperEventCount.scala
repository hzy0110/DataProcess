package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 每个城区场次数合计，最多
  * Created by Hzy on 2016/2/17.
  */
object ProperEventCount {
  def filename: String = "zhunian_DateEventYearCount_";

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_DateEventYearCount");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_simple.txt");

    //计算每个城区的场次
    val peCount = textFile.map(line => (line.substring(18,21),1)). //去重场次
     reduceByKey((a, b) => a + b).sortBy(_._2, false) //获取月份

    peCount.coalesce(1, shuffle = true).saveAsTextFile(filename + System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }
}
