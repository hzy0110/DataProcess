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
     println("-------------------textFile.count()=" + textFile.count())

     val tpDate = textFile.map(line => line.substring(22, 24))


     val pcDate = textFile.map(line => line.split("-")).
     filter(line => line.length > 1).
       map(line => (line(0).substring(22, 24) + ":" + line(1))).
       map(line => (line.toString.split(":")(0), line.toString.split(":")(1).split(","))).
       flatMap(line => (line._2.map(l2 => line._1+ l2))).
       map(line => (line,1)).
       reduceByKey((a, b) => a + b).
       sortByKey()



     pcDate.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
     println("Word Count program running results are successfully saved.");


   }

}
