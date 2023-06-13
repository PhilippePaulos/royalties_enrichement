package com.believe.royalties.processor.ingestion

import com.believe.royalties.config.ConfigLoader
import com.believe.royalties.models.{Album, Sale, Song}
import com.believe.royalties.processor.Processor
import com.believe.royalties.utils.spark.SparkHelper
import com.believe.royalties.utils.spark.sources.csv.CSVDataSource
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

case class StandardizedData(upc: String, isrc: String, label_name: String, album_name: String, song_id: BigInt,
                            song_name: String, artist_name: String, content_type: String,
                            total_net_revenue: Double, sales_country: String)


class SalesDataEnrichmentProcessor() extends Processor {

  import sparkSession.implicits._

  /**
   * The main process function of the program. It loads data from CSV files, and then enriches sales data
   * with additional album and song details.
   */
  override def process(): Unit = {
    val (albumsDF, salesDF, songsDF) = loadData()

    val enrichedDF = enrich_data(albumsDF, salesDF, songsDF)

    writeData(enrichedDF)
  }

  /**
   * Reads data from CSV files into Dataset with specific encoder.
   */
  def loadData(): (Dataset[Album], Dataset[Sale], Dataset[Song]) = {
    val conf = ConfigLoader.getApplicationConfig

    val salesDataSource = new CSVDataSource(conf.salesPath, Encoders.product[Sale])
    val salesDF = handleReadResult(SparkHelper.readFormDataSource(salesDataSource), "sales")

    val albumsDataSource = new CSVDataSource(conf.albumsPath, Encoders.product[Album])
    val albumsDF = handleReadResult(SparkHelper.readFormDataSource(albumsDataSource), "albums")

    val songsDataSource = new CSVDataSource(conf.songsPath, Encoders.product[Song])
    val songsDF = handleReadResult(SparkHelper.readFormDataSource(songsDataSource), "songs")

    (albumsDF, salesDF, songsDF)
  }

  /**
   * Handles results of CSV data read operations.
   */
  def handleReadResult[T](result: Either[Throwable, Dataset[T]], dataType: String): Dataset[T] = result match {
    case Left(error) =>
      logger.error(s"Error loading $dataType data")
      throw error
    case Right(df) => df
  }

  /**
   * Enriches sales data with additional album and song details.
   */
  def enrich_data(albumsDF: Dataset[Album], salesDF: Dataset[Sale], songsDF: Dataset[Song]): Dataset[StandardizedData] = {
    val songsSalesDF = joinSongsWithSales(salesDF, songsDF)

    val standardizedDF = joinWithAlbums(songsSalesDF, albumsDF)

    standardizedDF.as[StandardizedData]
  }

  /**
   * Joins the sales dataset with the songs dataset based on matching track ISRC codes and track IDs.
   */
  def joinSongsWithSales(salesDF: Dataset[Sale], songsDF: Dataset[Song]): DataFrame = {
    val songsSalesJoinCondition = salesDF("TRACK_ISRC_CODE") === songsDF("isrc") &&
      salesDF("TRACK_ID") === songsDF("song_id")

    salesDF
      .join(songsDF, songsSalesJoinCondition)
      .select($"PRODUCT_UPC", $"isrc", $"song_id", $"song_name", $"artist_name", $"content_type",
        $"NET_TOTAL".as("total_net_revenue"), $"TERRITORY")
  }

  /**
   * Joins the songs and sales DataFrame with the albums dataset based on matching UPCs and territories.
   */
  def joinWithAlbums(songsSalesDF: DataFrame, albumsDF: Dataset[Album]): DataFrame = {
    val albumSongsSalesJoinCondition = songsSalesDF("PRODUCT_UPC") === albumsDF("upc") &&
      songsSalesDF("TERRITORY") === albumsDF("country")

    songsSalesDF
      .join(albumsDF, albumSongsSalesJoinCondition)
      .select($"upc", $"isrc", $"label_name", $"album_name", $"song_id", $"song_name", $"artist_name",
        $"content_type", $"total_net_revenue", $"country".as("sales_country"))
  }

  def writeData(df: Dataset[StandardizedData]): Unit = {
    df.write.format("delta").mode("overwrite").save(ConfigLoader.getApplicationConfig.outputPath)
  }
}