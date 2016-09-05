package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 每个城区场次数合计，最多
  * Created by Hzy on 2016/2/17.
  */
object DeadAgeCount {
  def filename: String = "zhunian_DateEventYearCount_";

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_DateEventYearCount");
    val sc = new SparkContext(conf);
    val simpleFile = sc.textFile("/zhunian/zhunian_simple.txt");

    val daCount = simpleFile.map(line => (line.split("#")(1).split("_")(0).toInt))

    val daAvg =  daCount.
      reduce((a, b) => a + b).toDouble / simpleFile.count().toDouble

    val daMax = daCount.max

    val daMin = daCount.min

    println("最大年龄：" + daMax + " 最小年龄：" + daMin + " 平均年龄：" + daAvg);
  }
}
