package com.believe.royalties.utils.spark.sources.csv

import com.believe.royalties.utils.spark.SparkHelper.logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}

object CSVReader {

  /**
   * Reads a CSV file from the provided file path and returns it as a Dataset of type T.
   *
   * The function can log corrupted records based on the logCorrupted flag. If the flag is true,
   * the function uses a 'PERMISSIVE' mode to include corrupt records in a special column.
   * If the flag is false, the function uses a 'DROPMALFORMED' mode to drop any row containing at least one malformed CSV field.
   */
  def readCSV[T](filePath: String, encoder: Encoder[T], logCorrupted: Boolean, options: Map[String, String])
                (implicit sparkSession: SparkSession): Dataset[T] = {
    if (logCorrupted) {
      processPermissive(filePath, options, encoder)
    } else {
      processDropMalformed(filePath, options, encoder)
    }
  }

  /**
   * Reads a CSV file into a Dataset using the specified options.
   */
  private def processPermissive[T](resourcePath: String,
                                   options: Map[String, String],
                                   encoder: Encoder[T])(implicit sparkSession: SparkSession)
                                  : Dataset[T] = {
    val (corruptedDF, validDF) = handleCorruptRecords(resourcePath, options, encoder)

    logCorruptRecords(corruptedDF)

    validDF
  }


  /**
   * Reads a CSV file with permissive mode and filters the resulting DataFrame. The function separates the data into corrupt and valid records,
   * returning both as a tuple of Datasets.
   */
  def handleCorruptRecords[T](resourcePath: String, options: Map[String, String], encoder: Encoder[T],
                              corruptColumnName: String = "_corrupt_record")(implicit sparkSession: SparkSession): (Dataset[Row], Dataset[T]) = {
    val updatedSchema = encoder.schema.add(StructField(corruptColumnName, StringType, nullable = true))

    val df = sparkSession.read.options(options)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", corruptColumnName)
      .schema(updatedSchema).csv(resourcePath)

    val corruptedDF = df.filter(col(corruptColumnName).isNotNull)
    val validDF = df.filter(col(corruptColumnName).isNull).drop(corruptColumnName).as[T](encoder)

    (corruptedDF, validDF)
  }

  /**
   * Reads a CSV file into a DataFrame using the specified options and drops malformed records.
   */
  private def processDropMalformed[T](resourcePath: String,
                                      options: Map[String, String],
                                      encoder: Encoder[T])
                                     (implicit sparkSession: SparkSession): Dataset[T] = {
    sparkSession.read.options(options)
      .option("mode", "DROPMALFORMED")
      .schema(encoder.schema)
      .csv(resourcePath).as[T](encoder)
  }

  /**
   * Log the corrupt records.
   */
  private def logCorruptRecords(corruptedDF: Dataset[Row]): Unit = {
    corruptedDF.collect().foreach(row => logger.error(s"Corrupt record: $row"))
  }
}
