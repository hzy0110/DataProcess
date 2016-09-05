package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 亡者男女比例，亡者信佛比例，家属信佛比例
  * Created by Hzy on 2016/2/17.
  */
object DeadRatioCount {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_DateEventYearCount");
    val sc = new SparkContext(conf);
    val simpleFile = sc.textFile("/zhunian/zhunian_simple.txt");

    val lineCount = simpleFile.count.toDouble

    val mCount = simpleFile.map(line => (line.split("_")(1).split("!")(0), 1)).filter(line => line._1 == "男").count.toDouble
    val fCount = simpleFile.map(line => (line.split("_")(1).split("!")(0), 1)).filter(line => line._1 == "女").count.toDouble

    val selfBelieveCount = simpleFile.map(line => (line.split("!")(1).split("~")(0), 1)).filter(line => line._1 == "本有").count.toDouble
    val familyBelieveCount = simpleFile.map(line => (line.split("!")(1).split("~")(1), 1)).filter(line => line._1 == "属有").count.toDouble

    val mRatio = mCount / lineCount
    val fRatio = fCount / lineCount

    val sbeRatio = selfBelieveCount / lineCount
    val fBeRatio = familyBelieveCount / lineCount

    println("男性比例：" + mRatio + " 女性比例：" + fRatio + " 亡者信佛比例：" + sbeRatio + " 家属信佛比例：" + fBeRatio);
  }
}
