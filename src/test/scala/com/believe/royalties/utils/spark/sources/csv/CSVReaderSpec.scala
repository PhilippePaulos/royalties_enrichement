package com.believe.royalties.utils.spark.sources.csv

import com.believe.royalties.SharedSparkSession
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class CSVReaderSpec extends AnyFlatSpec with Matchers with MockitoSugar with SharedSparkSession {
  import spark.implicits._

  val encoder: Encoder[Person] = Encoders.product[Person]
  val options: Map[String, String] = Map("header" -> "true", "delimiter" -> ";")

  "CSVReader" should "load data from a valid CSV file" in {
    val filePath = "src/test/resources/data/people.csv"
    val actualDataset = CSVReader.readCSV[Person](filePath, encoder, logCorrupted = false, options)

    val expectedDataset: Dataset[Person] = Seq(Person("John", 30), Person("Jane", 25)).toDS()
    assert(actualDataset.collect() sameElements expectedDataset.collect())
  }

  it should "handle corrupted records in CSV file when handleCorrupted is set to true" in {
    val filePath = "src/test/resources/data/people_corrupted.csv"

    val (corruptedDF, rightDF) = CSVReader.handleCorruptRecords(filePath, options, encoder)
    assert(corruptedDF.collect().length == 1)
    assert(rightDF.collect() sameElements Seq(Person("John", 30)).toDS().collect())
  }

  it should "drop corrupted records in CSV file when handleCorrupted is set to false" in {
    val filePath = "src/test/resources/data/people_corrupted.csv"
    val actualDataset = CSVReader.readCSV[Person](filePath, encoder, logCorrupted = false, options)

    val expectedDataset: Dataset[Person] = Seq(Person("John", 30)).toDS()
    assert(actualDataset.collect() sameElements expectedDataset.collect())
  }

}