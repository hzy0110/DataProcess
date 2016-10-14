package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 亡者年龄最大，最小，平均
  * Created by Hzy on 2016/2/17.
  */
object DeadAgeCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_DeadAgeCount");
    val sc = new SparkContext(conf);
    val simpleFile = sc.textFile("/zhunian/zhunian_simple.txt");

    val daCount = simpleFile.map(line => line.split("#")(1).split("_")(0).toInt)
    val daAvg =  daCount.reduce((a, b) => a + b).toDouble / simpleFile.count().toDouble
    val daMax = daCount.max
    val daMin = daCount.min

    val byData = simpleFile.filter(line => line.split("!")(1).split("~")(0) == "本有").map(line => (line.split("!")(1).split("~")(0), line.split("#")(1).split("_")(0).toInt))
    val bwData = simpleFile.filter(line => line.split("!")(1).split("~")(0) == "本无").map(line => (line.split("!")(1).split("~")(0), line.split("#")(1).split("_")(0).toInt))

    val byAvg = byData.reduceByKey((a, b) => a + b).max._2.toDouble / byData.count().toDouble
    val bwAvg = bwData.reduceByKey((a, b) => a + b).max._2.toDouble / bwData.count().toDouble

    println("信最小年龄：" + byData.min + " 最大年龄：" + byData.max + " 平均年龄：" + byAvg);
    println("不信最小年龄：" + bwData.min + " 最大年龄：" + bwData.max + " 平均年龄：" + bwAvg);
    println("总最大年龄：" + daMax + " 最小年龄：" + daMin + " 平均年龄：" + daAvg);
  }
}
