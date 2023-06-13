package com.believe.royalties.config

import com.believe.royalties.utils.fs.FileHelper
import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try


case class ApplicationConfig(appName: String, master: String, albumsPath: String, salesPath: String, songsPath: String,
                             outputPath: String)

trait ConfigLoader {
  def loadConfig: Either[Throwable, ApplicationConfig]

  def getApplicationConfig: ApplicationConfig = loadConfig match {
    case Right(config) => config
    case Left(ex) => throw new Exception("Failed to load configuration", ex)
  }
}

object ConfigLoader extends ConfigLoader {
  private final val CONFIG_FILE = "application.yaml"

  override def loadConfig: Either[Throwable, ApplicationConfig] = {
    FileHelper.loadResource(CONFIG_FILE, getClass.getClassLoader).flatMap { inputStream =>
      Try {
        val yaml = new Yaml()
        val configData = yaml.load(inputStream).asInstanceOf[java.util.Map[String, Any]].asScala.toMap
        val sparkData = configData("spark").asInstanceOf[java.util.Map[String, String]].asScala.toMap
        val appData = configData("application").asInstanceOf[java.util.Map[String, String]].asScala.toMap
        ApplicationConfig(sparkData("app_name"), sparkData("master"), appData("albums_path"), appData("sales_path"),
          appData("songs_path"), appData("output_path"))
      }.toEither
    }
  }
}
