package com.believe.royalties.processor.ingestion

import com.believe.royalties.SharedSparkSession
import com.believe.royalties.models.{Album, Sale, Song}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class SalesDataEnrichmentProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar with SharedSparkSession with BeforeAndAfter{
  import spark.implicits._

  var processor: SalesDataEnrichmentProcessor = _

  before {
    processor = new SalesDataEnrichmentProcessor()
  }

  "enrich_data" should "return the standardized data with joined albums and sales" in {
    val salesData = Seq(Sale("upc1", "isrc1", BigInt("1"), "download", 0.60, "AUS")).toDS()
    val albumsData = Seq(Album("upc1", "album_name", "label1", "AUS")).toDS()
    val songsData = Seq(Song("isrc1", BigInt("1"), "Song1", "Artist1", "content1")).toDS()

    val result: Dataset[StandardizedData] = processor.enrich_data(albumsData, salesData, songsData)

    val expectedData = Seq(
      StandardizedData("upc1", "isrc1", "label1", "album_name", BigInt("1"), "Song1", "Artist1", "content1",
        0.60, "AUS")
    ).toDS()

    result.collect() should contain theSameElementsAs expectedData.collect()
  }

  "joinSongsWithSales" should "return the joined DataFrame of songs and sales" in {
    val salesData = Seq(Sale("upc1", "isrc1", BigInt("1"), "download", 0.60, "AUS")).toDS()
    val songsData = Seq(Song("isrc1", BigInt("1"), "Song1", "Artist1", "content1")).toDS()

    val result: DataFrame = processor.joinSongsWithSales(salesData, songsData)

    val expectedColumns = Seq(
      "PRODUCT_UPC", "isrc", "song_id", "song_name", "artist_name", "content_type", "total_net_revenue", "TERRITORY"
    )

    val expectedData = Seq(("upc1", "isrc1", BigInt("1"), "Song1", "Artist1", "content1", 0.60, "AUS"))

    result.columns should contain theSameElementsAs expectedColumns
    result.as[(String, String, BigInt, String, String, String, Double, String)].collect() should contain theSameElementsAs expectedData
  }

  "joinWithAlbums" should "return the joined DataFrame of songs, sales, and albums" in {
    val songsSalesData = Seq(
      ("upc1", "isrc1", BigInt("1"), "Song1", "Artist1", "content1", 0.60, "AUS")
    ).toDF("PRODUCT_UPC", "isrc", "song_id", "song_name", "artist_name", "content_type", "total_net_revenue", "TERRITORY")

    val albumsData = Seq(
      Album("upc1", "label1", "name", "AUS")
    ).toDS()

    val result: DataFrame = processor.joinWithAlbums(songsSalesData, albumsData)

    val expectedColumns = Seq("upc", "isrc", "label_name", "album_name", "song_id", "song_name", "artist_name",
      "content_type", "total_net_revenue", "sales_country")

    val expectedData = Seq(("upc1", "isrc1", "name", "label1", BigInt("1"), "Song1", "Artist1", "content1", 0.60, "AUS"))

    result.columns should contain theSameElementsAs expectedColumns
    result.as[(String, String, String, String, BigInt, String, String, String, Double, String)].collect() should contain theSameElementsAs expectedData
  }

  "handleReadResult" should "return the dataset if the result is Right" in {
    val dataset: Dataset[String] = Seq("data1", "data2").toDS()

    val result: Dataset[String] = processor.handleReadResult(Right(dataset), "test")

    result.collect() should contain theSameElementsAs Seq("data1", "data2")
  }

  it should "throw an exception if the result is Left" in {
    val exception = new RuntimeException("Test exception")

    val result = intercept[RuntimeException] {
      processor.handleReadResult(Left(exception), "test")
    }

    result.getMessage should equal("Test exception")
  }

  "enrichData" should "return the enriched dataset with the joined data" in {
    val albumsData = Seq(Album("upc1", "album1", "label1", "AUS")).toDS()
    val salesData = Seq(Sale("upc1", "isrc1", BigInt("1"), "download", 0.60, "AUS")).toDS()
    val songsData = Seq(Song("isrc1", BigInt("1"), "Song1", "Artist1", "content1")).toDS()

    val result: Dataset[StandardizedData] = processor.enrich_data(albumsData, salesData, songsData)

    val expectedData = Seq(
      StandardizedData("upc1", "isrc1", "label1", "album1", BigInt("1"), "Song1", "Artist1", "content1", 0.60, "AUS")
    )

    result.collect() should contain theSameElementsAs expectedData
  }
}
