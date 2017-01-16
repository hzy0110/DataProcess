package com.hzy

import org.apache.spark.SparkConf
/*import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}*/

/**
 * Created by zy on 2016/8/24.
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    /*if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc =  new StreamingContext(sparkConf, Seconds(60))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(60), 2)
    val wordCounts = words.map(word => (word, 1)).reduceByKey((a, b) => a + b)
    //println(wordCounts.count())
    //wordCounts.print()
    wordCounts.saveAsTextFiles("word_count_results_"+System.currentTimeMillis)
    println("---------------------------------start-------------------------------------------")
    ssc.start()
    ssc.awaitTermination()
    println("---------------------------------awaitTermination-------------------------------------------")*/
  }
}
