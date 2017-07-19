package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkForTesting {

  val sparkConf: SparkConf    = new SparkConf().setMaster("local[*]").setAppName("Tests")
  val sc: SparkContext        = SparkContext.getOrCreate(sparkConf)
  val session: SparkSession   = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext  = session.sqlContext

}
