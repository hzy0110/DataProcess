package com.hzy
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/**
 * Created by Hzy on 2016/2/17.
 */
object SparkWordCount {
  def filename:String  = "zhunian_";
  def main(args:Array[String]) {
//    if (args.length < 1) {
//      println("Usage:SparkWordCount FileName");
//      System.exit(1);
//    }
    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("/zhunian/zhunian.txt");
    println("textFile.count()" + textFile.count())
/*    var mapfirst = textFile.flatMap(line => line.split("-")(1))
    println("mapfirst.count()" + mapfirst.count())
    var mapfirst1 = textFile.flatMap(line => line.split("-"))
    println("mapfirst1.count()" + mapfirst1.count())*/

    /*   var mapfirst2 = textFile.map(line => line.split("-")(1))
       println("mapfirst.count()" + mapfirst2.count())
       var mapfirst3 = textFile.map(line => line.split("-"))
       println("mapfirst1.count()" + mapfirst3.count())*/

    //val textFile = sc.textFile("file://H:/TestData/zhunian.txt");
    val nameDate = textFile.map(line => line.split("-")(1))
    val wordCounts = nameDate.flatMap(line => line.split(",")).map(
      word => (word, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    //.coalesce(1, shuffle = true)把多个文件合并一个
    wordCounts.coalesce(1, shuffle = true).saveAsTextFile(filename +System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");


//    println(nameDate.count())
    //val nameValue = nameDate.map(nd => (nd.split(",")))
    //val wordCounts = nameDate.flatMap(line => line.split(",")).map(
     // word => (word, 1)).reduceByKey((a, b) => a + b)
    //val wordCounts = nameValue.map(name => name).reduceByKey((a, b) => a + b)
   /* var mapfirst = textFile.flatMap(line => line.split("-")(1))
    val wordCounts = mapfirst.map(line => line.toString.split(",")).map(
      word => (word, 1)).reduceByKey((a, b) => a + b)*/

    //wordCounts.saveAsTextFile(filename +System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }

}
