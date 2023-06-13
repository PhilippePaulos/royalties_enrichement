package com.believe.royalties

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

trait SharedSparkSession extends BeforeAndAfterAll { self: org.scalatest.Suite =>
  @transient implicit lazy val spark: SparkSession = {
    SparkSession.builder
      .appName("Spark Testing")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (!spark.sparkContext.isStopped) {
      spark.stop()
    }
  }
}