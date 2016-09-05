package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 哪个城区人最多（日均）
  * Created by Hzy on 2016/2/17.
  */
object ProperEventCount {
  def filename: String = "zhunian_ProperEventCount_";

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_ProperEventCount");
    val sc = new SparkContext(conf);
    val simpleFile = sc.textFile("/zhunian/zhunian_simple.txt");
    val detailedFile = sc.textFile("/zhunian/zhunian_detailed.txt");

    //计算每个城区的场次
    val xhSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split(":")(1).split("-")(0) == "西湖")
    val scSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split(":")(1).split("-")(0) == "上城")
    val jgSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split(":")(1).split("-")(0) == "江干")
    val gsSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split(":")(1).split("-")(0) == "拱墅")
    val xcSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split(":")(1).split("-")(0) == "下城")
    val bjSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split(":")(1).split("-")(0) == "滨江")

    val detailedData = detailedFile.map(line => (line.split(":")(0), line))

    val xhData = xhSimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val scData = scSimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val jgData = jgSimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val gsData = gsSimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val xcData = xcSimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val bjData = bjSimpleData.join(detailedData).map(line => line._2._2.split("-"))

    val xhDateCount = xhData.count().toDouble
    val scDateCount = scData.count().toDouble
    val jgDateCount = jgData.count().toDouble
    val gsDateCount = gsData.count().toDouble
    val xcDateCount = xcData.count().toDouble
    val bjDateCount = bjData.count().toDouble

    val xhPeopleCount = xhData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算西湖区人数
    val scPeopleCount = scData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算上城区人数
    val jgPeopleCount = jgData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算江干区人数
    val gsPeopleCount = gsData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算拱墅区人数
    val xcPeopleCount = xcData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算下城区人数
    //val bjPeopleCount = bjData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算滨江区人数

    println("西湖区日均人数=" + xhPeopleCount / xhDateCount * 8)
    println("上城区日均人数=" + scPeopleCount / scDateCount * 8)
    println("江干区日均人数=" + jgPeopleCount / jgDateCount * 8)
    println("拱墅区日均人数=" + gsPeopleCount / gsDateCount * 8)
    println("下城区日均人数=" + xcPeopleCount / xcDateCount * 8)
    //println("滨江区日均人数=" + bjPeopleCount / bjDateCount * 8)

    //xhData.coalesce(1, shuffle = true).saveAsTextFile(filename + System.currentTimeMillis());


  }
}
