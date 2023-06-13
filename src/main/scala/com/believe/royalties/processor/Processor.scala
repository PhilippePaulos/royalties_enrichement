package com.believe.royalties.processor

import com.believe.royalties.utils.LoggerWrapper
import com.believe.royalties.utils.spark.SparkSessionWrapper
import org.apache.spark.sql.SparkSession

abstract class Processor(implicit val sparkSession: SparkSession = SparkSessionWrapper.spark) extends LoggerWrapper {

  def main_process(): Unit = {
    logger.debug("Starting main_process()")
    process()

    logger.debug("End of main_process()")
    sparkSession.stop()
  }

  def process(): Unit

}
