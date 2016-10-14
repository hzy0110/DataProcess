package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 哪个城区人最多（场均）
  * Created by Hzy on 2016/2/17.
  */
object ProperPeopleCount {
  def filename: String = "zhunian_ProperPeopleCount_";

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_ProperPeopleCount");
    val sc = new SparkContext(conf);
    val simpleFile = sc.textFile("/zhunian/zhunian_simple.txt");

    //获取每个城区场次数
    val peCount = simpleFile.map(line => (line.substring(21,23),1)).
     reduceByKey((a, b) => a + b).sortBy(_._2, false)

    peCount.coalesce(1, shuffle = true).saveAsTextFile(filename + System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }
}
