package com.believe.royalties

import com.believe.royalties.processor.{Processor, ProcessorFactory}


object Main {
  def main(args: Array[String]): Unit = {
    val processorType = if (args.isEmpty) "ingestion" else args(0)
    val processor: Processor = ProcessorFactory.createProcessor(processorType)

    processor.main_process()
  }
}

