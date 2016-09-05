package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 亡者是否信佛与家属是否信佛与参与人数关系，按天平均
  * Created by Hzy on 2016/2/17.
  */
object DeadPeopleCount {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_ProperEventCount");
    val sc = new SparkContext(conf);
    val simpleFile = sc.textFile("/zhunian/zhunian_simple.txt");
    val detailedFile = sc.textFile("/zhunian/zhunian_detailed.txt");

    //计算四种情况
    val wwSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split("!")(1).split(";")(0) == "本无~属无")
    val ywSimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split("!")(1).split(";")(0) == "本有~属无")
    val wySimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split("!")(1).split(";")(0) == "本无~属有")
    val yySimpleData = simpleFile.map(line => (line.split(":")(0), line)).filter(line => line._2.split("!")(1).split(";")(0) == "本有~属有")

    val detailedData = detailedFile.map(line => (line.split(":")(0), line))

    val wwData = wwSimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val ywData = ywSimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val wyData = wySimpleData.join(detailedData).map(line => line._2._2.split("-"))
    val yyData = yySimpleData.join(detailedData).map(line => line._2._2.split("-"))


    val wwDateCount = wwData.count().toDouble
    val ywDateCount = ywData.count().toDouble
    val wyDateCount = wyData.count().toDouble
    val yyDateCount = yyData.count().toDouble

    val wwPeopleCount = wwData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算西湖区人数
    val ywPeopleCount = ywData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算上城区人数
    val wyPeopleCount = wyData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算江干区人数
    val yyPeopleCount = yyData.filter(line => line.length > 1).map(line => line(1).split(",").length).reduce((a, b) => a + b).toDouble//计算拱墅区人数

    println("本无~属无日均人数=" + wwPeopleCount / wwDateCount * 8)
    println("本有~属无日均人数=" + ywPeopleCount / ywDateCount * 8)
    println("本无~属有日均人数=" + wyPeopleCount / wyDateCount * 8)
    println("本有~属有日均人数=" + yyPeopleCount / yyDateCount * 8)
  }
}
