package com.believe.royalties.utils.spark

import com.believe.royalties.SharedSparkSession
import com.believe.royalties.utils.spark.sources.DataSource
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class SparkHelperSpec extends AnyFlatSpec with Matchers with MockitoSugar with SharedSparkSession {

  import spark.implicits._

  "readFormDataSource" should "return the dataset from the data source when it loads successfully" in {
    val mockDataSource = mock[DataSource[String]]
    val dataset = Seq("test").toDS()

    when(mockDataSource.load()).thenReturn(dataset)

    val result = SparkHelper.readFormDataSource(mockDataSource)

    result should equal (Right(dataset))
  }

  it should "return an error when the data source fails to load" in {
    val mockDataSource = mock[DataSource[String]]
    val exception = new RuntimeException("Loading error")
    when(mockDataSource.load()).thenThrow(exception)

    val result = SparkHelper.readFormDataSource(mockDataSource)

    val errorMessage = result.swap.getOrElse(fail("Expected Left but got Right"))
    errorMessage shouldBe exception
    errorMessage.getMessage shouldBe "Loading error"
  }
}