package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计算哪个时段最常出现哪些人
  * Created by Hzy on 2016/2/17.
  */
object TimePeriodPeopleCount {
   def filename:String  = "zhunian_TimePeriodPeopleCount_";
   def main(args:Array[String]) {
     val conf = new SparkConf().setAppName("zhunian_TimePeriodPeopleCount");
     val sc = new SparkContext(conf);
     val textFile = sc.textFile("/zhunian/zhunian_detailed.txt");
     val pcDate = textFile.map(line => line.split("-")).
     filter(line => line.length > 1).
       map(line => (line(0).substring(22, 24) + ":" + line(1))).//拼接时段和人员
       map(line => (line.split(":")(0), line.split(":")(1).split(","))).//拆分时段和分割人员
       flatMap(line => (line._2.map(l2 => line._1+ l2))).//把人员单个拆分到一个数组
       map(line => (line,1)).
       reduceByKey((a, b) => a + b).
       sortByKey()
     pcDate.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
     println("Word Count program running results are successfully saved.");


   }

}
