package com.believe.royalties.config

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class ConfigLoaderSpec extends AnyWordSpec with Matchers {

  "ConfigLoader" should {

    val expectedConfig = ApplicationConfig(
      appName = "MyApp",
      master = "local[*]",
      albumsPath = "data/albums.csv",
      salesPath = "data/sales.csv",
      songsPath = "data/songs.csv",
      outputPath = "output/"
    )

    "load the application configuration" in {
      val stubConfigLoader = new StubConfigLoader(config = Some(expectedConfig))

      val actualConfig = stubConfigLoader.getApplicationConfig

      actualConfig shouldBe expectedConfig

    }

    "throw an exception if the configuration loading fails" in {
      val stubConfigLoader = new StubConfigLoader(exception = Some(new RuntimeException("Test exception")))

      val exception = intercept[Exception] {
        stubConfigLoader.getApplicationConfig
      }

      exception.getMessage shouldBe "Failed to load configuration"
      exception.getCause.getMessage shouldBe "Test exception"
    }
  }

  class StubConfigLoader(config: Option[ApplicationConfig] = None,
                         exception: Option[Throwable] = None) extends ConfigLoader {
    override def loadConfig: Either[Throwable, ApplicationConfig] = {
      exception.map(Left(_)).getOrElse(Right(config.get))
    }
  }
}
