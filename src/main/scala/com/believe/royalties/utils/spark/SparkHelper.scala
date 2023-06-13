package com.believe.royalties.utils.spark

import com.believe.royalties.utils.LoggerWrapper
import com.believe.royalties.utils.spark.sources.DataSource
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

object SparkHelper extends LoggerWrapper {

  def readFormDataSource[T](dataSource: DataSource[T]) (implicit sparkSession: SparkSession): Either[Throwable, Dataset[T]] = {
    Try {
      dataSource.load()
    }.toEither
  }
}
