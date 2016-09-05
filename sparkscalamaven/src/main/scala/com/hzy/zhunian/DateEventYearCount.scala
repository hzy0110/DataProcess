package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * X年产生X场
  * Created by Hzy on 2016/2/17.
  */
object DateEventYearCount {
   def filename:String  = "zhunian_DateEventYearCount_";
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_DateEventYearCount");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");


    //计算每年场次数
    val deyCount = textFile.map(line => (line.split(":")(0))).distinct(). //去重场次
      map(line => (line.substring(7,11) ,1)).reduceByKey((a, b) => a + b) //获取年份

    deyCount.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");


  }

}
