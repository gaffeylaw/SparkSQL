package com.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///Users/luozhenfei1/IdeaProjects/SparkSQL/data/output/access.log")

    //accessRDD.take(10).foreach(println)

    //RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    //accessDF.printSchema()

    /*
    *如果出现如下错误:
    * java.lang.RuntimeException: Error while encoding: java.lang.RuntimeException:
    * java.lang.Integer is not a valid external type for schema of string
    * 解决办法:
    * 是因为output/access.log文件里面出现了一些不符合规则的数据导致，需要去把这个文件更正
    * */
    //accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
    .partitionBy("day").save("file:///Users/luozhenfei1/IdeaProjects/SparkSQL/data/clean")

    spark.stop
  }
}
