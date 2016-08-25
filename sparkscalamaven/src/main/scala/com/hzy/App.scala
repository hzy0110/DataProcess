package com.hzy

import org.apache.spark.{SparkContext, SparkConf}

/**
 * @author ${user.name}
 */
object App {
  /*
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext()
    val fileName = "/home/hzy/tmp/sparktestfile.txt"
    val rdd1 = sc.textFile(fileName).flatMap(l => l.split(" ")).map(w => (w, 1))
    rdd1.reduceByKey(_ + _).foreach(println)

    var i: Int = 0
    while ( i < 10 ) {
      Thread.sleep(10000)
      i = i + 1
    }
  }

}
