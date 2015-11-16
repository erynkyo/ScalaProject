package org.sparkscala.stundy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by china_015 on 15/11/16.
 */
object RunSpark {
  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("RunSpark").setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)
    sc.addJar("/Users/china_015/GitHub/ScalaProject/out/artifacts/SparkScala_jar")
    val line = sc.textFile("/User/China_015/Documents/123.rtf")

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

    sc.stop()
  }
}
