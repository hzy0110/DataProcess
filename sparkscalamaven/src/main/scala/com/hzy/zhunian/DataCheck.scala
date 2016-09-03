package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据校验
 * Created by Hzy on 2016/2/17.
 */
object DataCheck {
  def filename:String  = "zhunian_DataCheck_";
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_DataCheck");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");


    val zndCount = textFile.map(line => (line.split("-")(0))).collect
    val zndCountd = textFile.map(line => (line.split("-")(0))).distinct().collect
    //zndCount.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
    //zndCountd.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
  }

}
