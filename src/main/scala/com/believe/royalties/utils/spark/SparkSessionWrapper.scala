package com.believe.royalties.utils.spark

import com.believe.royalties.config.ConfigLoader
import org.apache.spark.sql.SparkSession


object SparkSessionWrapper {

  @transient lazy val spark: SparkSession = {
    SparkSession.builder()
      .master(ConfigLoader.getApplicationConfig.master)
      .appName(ConfigLoader.getApplicationConfig.appName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }
}