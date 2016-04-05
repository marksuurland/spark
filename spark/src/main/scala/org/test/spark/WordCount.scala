package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import akka.actor

object WordCount {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
   
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("spark://cloudera-003.fusion.ebicus.com:7077")
    val sc = new SparkContext(conf)
		
    //Read some example file to a test RDD
    val file = sc.textFile("hdfs://cloudera-003.fusion.ebicus.com:8020/user/hive/warehouse/otravo.db/person")
    
    val counts = file.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
                 
    counts.saveAsTextFile("hdfs://cloudera-003.fusion.ebicus.com:8020/output")
  }

}