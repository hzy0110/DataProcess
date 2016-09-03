package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 出勤率
  * Created by Hzy on 2016/2/17.
  */
object AttendanceRatioCount {
  def filename: String = "zhunian_AttendanceRatioCount_";

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_AttendanceRatioCount");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");
    val eDate = textFile.map(line => line.split(":")(0)).distinct().count().toDouble
    val pcDate = textFile.map(line => line.split("-")).
      filter(line => line.length > 1).
      map(line => (line(0).split(":")(0) + ":" + line(1))).
      map(line => (line.split(":")(0), line.split(":")(1).split(","))).
      flatMap(line => (line._2.map(l2 => l2 + "-" + line._1))).distinct().
      map(line => (line.split("-")(0), 1)).
      reduceByKey((a, b) => a + b).
      map(line => (line._1, line._2.toDouble / eDate)).
      sortBy(_._2, false)
    pcDate.coalesce(1, shuffle = true).saveAsTextFile(filename + System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }
}
