package com.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用
  * 使用时需要通过--jars 把mysql的驱动传递到classpath
  *
  * 测试失败：
  * Exception in thread "main" org.apache.spark.sql.catalyst.analysis.NoSuchTableException:
  * Table or view 'wordcount' not found in database 'default';
  *
  * 解决办法：
  * cp ~/app/hive-1.1.0-cdh5.7.0/conf/hive-site.xml ~/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/conf/
  */
object HiveContextApp {

  def main(args: Array[String]) {
    //1)创建相应的Context
    val sparkConf = new SparkConf()

    //在测试或者生产中，AppName和Master我们是通过脚本进行指定
    sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2)相关的处理:
    hiveContext.table("wordcount").show

    //3)关闭资源
    sc.stop()
  }

}