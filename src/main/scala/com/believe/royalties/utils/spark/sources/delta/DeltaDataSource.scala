package com.believe.royalties.utils.spark.sources.delta

import com.believe.royalties.utils.spark.sources.DataSource
import jdk.jshell.spi.ExecutionControl.NotImplementedException
import org.apache.spark.sql.{Dataset, SparkSession}

class DeltaDataSource[T](df: Dataset[T], outputPath: String) extends DataSource[T] {
  override def load()(implicit sparkSession: SparkSession): Dataset[T] = throw new NotImplementedException("load() not implemented")

  override def write(): Unit = {
    df.write.format("delta").mode("overwrite").save(outputPath)
  }
}