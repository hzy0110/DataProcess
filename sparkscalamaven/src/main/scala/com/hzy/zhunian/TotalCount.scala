package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 总数的统计
  * Created by Hzy on 2016/2/17.
  */
object TotalCount {
  def filename: String = "zhunian_TotalCount_";

  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      println("Usage:SparkWordCount FileName");
    //      System.exit(1);
    //    }
    val conf = new SparkConf().setAppName("zhunian_TotalCount");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");

    //总场次数
    val EventTotalCount = 100;

    //总人次（不去重）
    val propleTotalCount = textFile.map(line => line.split("-")).map(line =>{
      if(line.length > 1){
        line(1)
      }
    }).map(line => line.toString.split(",").length).map(word => ("p", word)).reduceByKey((a, b) => a + b).max()._2
    val allp: Array[String] = Array()
    //总人数去重
    val propleTotalCountD = textFile.map(line => line.split("-")).map(line =>{
      if(line.length > 1){
        line(1)
      }
    }).flatMap(line => line.toString.split(",")).distinct().count();

    val timePeriodTotalCount = textFile.map(line => line.split("-")).map(line =>{
      if(line.length > 1){
        line(1)
      }
    }).map(line => line.toString.split(",").length * 3).map(word => ("p", word)).reduceByKey((a, b) => a + b).max()._2

    val properTotal = "西湖，江干，上城，下城，拱墅，滨江"

    println("------------" +
      " 总场数："+ EventTotalCount +
      " 总人次：" +  propleTotalCount +
      " 总人数：" + propleTotalCountD +
    " 总时数：" +  propleTotalCountD * timePeriodTotalCount + "小时" +
    " 总日数：" + propleTotalCountD * timePeriodTotalCount /8 +
    " 涉及城区：" + properTotal +
    " 为"+ propleTotalCount + "位亡者助念")
  }

}
