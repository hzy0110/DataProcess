package com.hzy.zhunian

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Hzy on 2016/2/17.
  */
object TimePeriodCount {
   def filename:String  = "zhunian_TimePeriodCount_";
   def main(args:Array[String]) {
     //    if (args.length < 1) {
     //      println("Usage:SparkWordCount FileName");
     //      System.exit(1);
     //    }
     val conf = new SparkConf().setAppName("zhunian_TimePeriodCount");
     val sc = new SparkContext(conf);
     val textFile = sc.textFile("/zhunian/zhunian.txt");
     println("-------------------textFile.count()=" + textFile.count())

     val tpDate = textFile.map(line => line.substring(22, 24))
     //val pcDate = textFile.map(line => line.split("-")(1).split(","))
     //分割-
     val pcDate = textFile.map(line => line.split("-"))
     //val pcDate1 = textFile.map(line => line.split("-").length>0)
     //判断长度大于1的。。放入
     //val pcDate2 = pcDate.filter(line => line.length > 1).map(line => line(1).split(","))
//     val pcDate2 = pcDate.filter(line => line.length > 1).map(line => (line(0).substring(22, 24) + ":" + line(1).split(",").length))
//     val pcDate3 = pcDate.filter(line => line.length == 1).map(line => (line(0).substring(22, 24) + ":" + "0"))

     //取时段+:+数量，存如到pcd4
     val pcDate4 = pcDate.map(line => (line(0).substring(22, 24) + ":" + {
       //println("line(0).substring(22, 24)=" + line(0).substring(22, 24))
       if( line.length > 1){
         line(1).split(",").length
       }
       else{
         0
       }
     }))

     //pcd4分割：数组形式放入到pcd5
     val pcDate5 = pcDate4.map(line => ((line.split(":")(0),line.split(":")(1).toDouble)))

     /*    var x = ""
          pcDate.collect().foreach(e => {
            if (e.length > 1) {
               x += e(0) + e(1).split(",").length
            }
            else {
              x += e(0) + "0"
            }
          });*/


     //     pcDate.collect().foreach(e => {
     //       val (x) = e.length
     //       println(x)
     //     });

     //     pcDate1.collect().foreach(e => {
     //       val (x) = e
     //       println(x)
     //     });

/*     pcDate2.collect().foreach(e => {
       val (x) = e
       println(x)
     });*/

     //     for(v <- pcDate){
     //       println("v.l=" +  v.length)
     //       println("v=" +  v)
     //     }

/*     pcDate4.collect().foreach(e => {
/*       val (x) = e.length
       for (i <- 0 until e.length)
        println(i+"："+e(i))
       println(x)*/
       println("e=" + e)
     });*/


//     val rdd1 = pcDate5.reduceByKey((a, b) => a + b)
//     var rdd2 = pcDate5.reduceByKey((x,y) => x + y)

     //计算每个时段总人数
     val pdCounts = pcDate5.reduceByKey((x,y) => x + y).sortBy(_._1)
     //计算每个时段出现次数
     val tdCounts = tpDate.map(word => (word, 1.toDouble)).reduceByKey((a, b) => a + b).sortBy(_._1)

     //val ttt = pdCounts.map(x => (x,tdCounts.map(x => x)))
     val tpAll = pdCounts ++  tdCounts
     /*     val tpAvg = Array

          //已计算出平均值，但是不知道怎么save
          for (i <- 0 until tdCounts.collect.length){
            val avgAge : Double = pdCounts.collect()(i)._2.toDouble / tdCounts.collect()(i)._2.toDouble
            val tpAvg1 =  Array(pdCounts.collect()(i)._1, avgAge)
            tpAvg +: tpAvg1
          }*/


      //val wordCounts = pcDate4.flatMap(line => line.split(":")).map(word => (word, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
      val tpAvg = tpAll.reduceByKey((a, b) => a / b).sortBy(_._2, false)
     //.coalesce(1, shuffle = true)把多个文件合并一个
     tpAvg.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
     println("Word Count program running results are successfully saved.");


   }

}
