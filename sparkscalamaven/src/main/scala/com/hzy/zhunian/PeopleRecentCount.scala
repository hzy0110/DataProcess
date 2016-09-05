package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计算近10场出现人员
 * Created by Hzy on 2016/2/17.
 */
object PeopleRecentCount {
  def filename:String  = "zhunian_PeopleRecentCount_";
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("zhunian_PeopleRecentCount");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");

    val evDate = textFile.map(line => line.split("-")).
      filter(line => line.length > 1). //过滤无人员行
      map(line => (line(0).split(":")(0),line(1))). //取得场次名次
       sortBy(_._1,false). //倒序场次名
       take(1).map(line => line._1.substring(4,6)) //取最后一场No数

    //设定最近场次数
    val no = evDate(0).toInt - 10;

    val pcDate = textFile.map(line => line.split("-")).
     filter(line => line.length > 1).
     filter(line => line(0).substring(4,6).toInt > no).
       //map(line => (line(0).split(":")(0) + ":" + line(1))).//拼接时段和人员
       //map(line => (line.split(":")(0), line.split(":")(1).split(","))).//拆分时段和分割人员
       flatMap(line => (line(1).split(","))).distinct()//把人员单个拆分到一个数组
       //map(line => (line,1)).
      //reduceByKey((a, b) => a + b).
      //sortByKey()

    /*val pcDate = textFile.map(line => line.split("-")).
      filter(line => line.length > 1).
      map(line => (line(0).split(":")(0) + ":" + line(1))).
      map(line => (line.split(":")(0), line.split(":")(1).split(","))).
      flatMap(line => (line._2.map(l2 => l2 + "-" +line._1))).distinct().
      map(line => (line.split("-")(0),1)).
      reduceByKey((a, b) => a + b).
      sortBy(_._2,false)*/
    pcDate.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }

}
