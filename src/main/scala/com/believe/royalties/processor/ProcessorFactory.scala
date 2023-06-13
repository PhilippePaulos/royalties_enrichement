package com.believe.royalties.processor

import com.believe.royalties.processor.ingestion.SalesDataEnrichmentProcessor

object ProcessorFactory {

  /**
   * Creates a specific instance of the Processor based on the provided processorType.
   */
  def createProcessor(processorType: String): Processor = {
    processorType match {
      case "ingestion" => new SalesDataEnrichmentProcessor()
      case _ => throw new IllegalArgumentException(s"Unsupported processor type: $processorType")
    }
  }
}