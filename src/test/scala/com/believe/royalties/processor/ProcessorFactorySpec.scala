package com.believe.royalties.processor

import com.believe.royalties.processor.ingestion.SalesDataEnrichmentProcessor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

class ProcessorFactorySpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  val testData: TableFor2[String, Either[Processor, Class[_]]] = Table(
    ("processorType", "expectedInstance"),
    ("ingestion", Left(new SalesDataEnrichmentProcessor())),
    ("unknown", Right(classOf[IllegalArgumentException]))
  )

  "createProcessor" should "create the correct instance of Processor" in {
    forAll(testData) { (processorType: String, expectedInstance: Any) =>
      expectedInstance match {
        case Right(_) =>
          intercept[IllegalArgumentException] {
            ProcessorFactory.createProcessor(processorType)
          }
        case Left(expectedProcessor) =>
          val result = ProcessorFactory.createProcessor(processorType)
          result.getClass should be(expectedProcessor.getClass)
      }
    }
  }
}