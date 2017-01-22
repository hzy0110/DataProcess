package com.hzy.hbase


import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hzy on 2017/1/10.
  */


object Test {

  val cat = s"""{
               |"table":{"namespace":"default", "name":"serv_msg"},
               |"rowkey":"key",
               |"columns":{
               |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
               |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
               |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
               |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
               |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
               |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
               |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
               |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
               |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
               |}
               |}""".stripMargin

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    //    sparkConf.setMaster("spark://master:7077")
    sparkConf.setMaster("yarn-client");
    sparkConf.set("spark.yarn.appMasterEnv.CLASSPATH",
      "$CLASSPATH:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*")
    sparkConf.set("spark.executor.memory","256M")
    sparkConf.setJars(List(
      "hdfs://master/zhunian/sparkscalamaven-1.0-SNAPSHOT.jar",
      //            "hdfs://master/zhunian/json4s-ast_2.10-3.2.10.jar",
      //            "hdfs://master/zhunian/json4s-core_2.10-3.2.10.jar",
      //            "hdfs://master/zhunian/json4s-jackson_2.10-3.2.10.jar",
      "hdfs://master/zhunian/shc-core-1.0.2-1.6-s_2.10-SNAPSHOT.jar",
      "hdfs://master/zhunian/shc-examples-1.0.2-1.6-s_2.10-SNAPSHOT.jar",
      "hdfs://master/zhunian/hbase-client-1.2.0-cdh5.9.0.jar"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
//                .format("org.apache.hadoop.hbase.spark")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }


    val df = withCatalog(cat)
    df.show
    df.registerTempTable("serv_msg")
    //    val c = sqlContext.sql("select count(col1) from table1 where col0 < 'row050'")
    val c = sqlContext.sql("select col1 from serv_msg")
    c.show()
    /*val res = df.select("col1")
    //res.save("hdfs://master:9000/user/yang/a.txt")
    res.show()
    df.registerTempTable("table4")
    sqlContext.sql("select count(col0),sum(col1) from table4 where col1>'20' and col1<'26' ").show
    println("-----------------------------------------------------");
    sqlContext.sql("select count(col1),avg(col1) from table4").show*/
  }
}
