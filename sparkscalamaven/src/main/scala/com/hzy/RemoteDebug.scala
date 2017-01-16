package com.hzy

/**
  * Created by hzy on 2017/1/16.
  */
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf,SparkContext}

object RemoteDebug {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("pi")
    //conf.setMaster("spark://ods18:7077")
    conf.setMaster("yarn-client")
    //conf.setMaster("spark://192.168.3.185:17077")
    conf.set("spark.executor.memory","128M")
    conf.set("spark.yarn.appMasterEnv.CLASSPATH",
      "$CLASSPATH:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*")
    conf.setJars(List("hdfs://ods18/zhunian/sparkscalamaven-1.0-SNAPSHOT.jar"))
    val spark = new SparkContext(conf)
    val slices = 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = Math.random * 2 - 1
      val y = Math.random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
