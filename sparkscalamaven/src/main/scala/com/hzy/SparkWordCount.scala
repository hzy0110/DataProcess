package com.hzy
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/**
 * Created by Hzy on 2016/2/17.
 */
object SparkWordCount {
  def filename:String  = "word_count_results_";
  def main(args:Array[String]) {
    if (args.length < 1) {
      println("Usage:SparkWordCount FileName");
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile(args(0));
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(
      word => (word, 1)).reduceByKey((a, b) => a + b)
    //print the results,for debug use.
    //println("Word Count program running results:");
    //wordCounts.collect().foreach(e => {
    //val (k,v) = e
    //println(k+"="+v)
    //});
    wordCounts.saveAsTextFile(filename +System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }

}
