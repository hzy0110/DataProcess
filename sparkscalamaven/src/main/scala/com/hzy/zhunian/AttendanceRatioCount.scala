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
    conf.setMaster("spark://192.168.3.185:17077")
    conf.set("spark.executor.memory","512M")
    //conf.set("spark.executor.extraClassPath","/sparkscalamaven/target/")
    //conf.setJars(Array("/sparkscalamaven/target/sparkscalamaven-1.0-SNAPSHOT.jar")) //这行没有写,加上就好了

    val sc = new SparkContext(conf);
    sc.addJar("/sparkscalamaven/target/sparkscalamaven-1.0-SNAPSHOT.jar")
    val textFile = sc.textFile("hdfs:/zhunian/zhunian_detailed.txt");
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
