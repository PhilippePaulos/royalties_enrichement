package com.believe.royalties.utils.spark.sources

import org.apache.spark.sql.{Dataset, SparkSession}

trait DataSource[T] {
  def load()(implicit sparkSession: SparkSession): Dataset[T]
  def write(): Unit
}
