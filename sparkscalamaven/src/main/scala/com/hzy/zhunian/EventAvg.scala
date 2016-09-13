package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计算场均人数
  * Created by Hzy on 2016/2/17.
  */
object EventAvg {
  def filename: String = "zhunian_EventAvg_";

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_EventAvg");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");


    //计算每行人数
    val znpCount = textFile.filter(line => line.split("-").length > 1).map(line => (line.split(":")(0),line.split(",").length.toDouble)).reduceByKey((a, b) => a + b)
    //获取每场时段数量
    val zndCount = textFile.map(line => line.split(":")(0)).map(word => (word, 1.toDouble)).reduceByKey((a, b) => a + b)



    //加和2个数组
    val znALL = znpCount ++ zndCount

    val eventAvg = znALL.reduceByKey((a, b) => a / b * 8).sortBy(_._2, false)

    //val wordCounts = znCount.map(word => (word, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    //val tpAvg = tpAll.reduceByKey((a, b) => a / b).sortBy(_._2, false)
    //.coalesce(1, shuffle = true)把多个文件合并一个
    eventAvg.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");


  }

}
