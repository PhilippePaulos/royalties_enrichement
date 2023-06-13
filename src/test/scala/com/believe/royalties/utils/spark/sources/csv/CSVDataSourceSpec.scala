package com.believe.royalties.utils.spark.sources.csv

import com.believe.royalties.SharedSparkSession
import com.believe.royalties.utils.spark.sources.DataSource
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Person(name: String, age: Int)

class CSVDataSourceSpec extends AnyFlatSpec with Matchers with SharedSparkSession {
  val encoder: Encoder[Person] = Encoders.product[Person]
  val filePath = "src/test/resources/data/people.csv"
  val options: Map[String, String] = Map("header" -> "true", "delimiter" -> ";")

  "CSVDataSource" should "load data from a CSV file" in {
    val expectedDataset: Dataset[Person] = spark.read.options(options)
      .schema(Encoders.product[Person].schema).csv(filePath).as[Person](encoder)

    val dataSource: DataSource[Person] = new CSVDataSource[Person](filePath, encoder, options)
    val actualDataset: Dataset[Person] = dataSource.load()

    assert(actualDataset.collect() sameElements expectedDataset.collect())
  }
}
