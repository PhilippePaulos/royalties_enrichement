package com.believe.royalties.utils.spark.sources.csv

import com.believe.royalties.utils.spark.sources.DataSource
import jdk.jshell.spi.ExecutionControl.NotImplementedException
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

class CSVDataSource[T](filePath: String, encoder: Encoder[T], options: Map[String, String] = Map("header" -> "true", "delimiter" -> ";"),
                       logCorrupted: Boolean = true) extends DataSource[T] {
  override def load()(implicit sparkSession: SparkSession): Dataset[T] = {
    CSVReader.readCSV[T](filePath, encoder, logCorrupted, options)
  }

  override def write(): Unit = throw new NotImplementedException("write() not implemented")
}
