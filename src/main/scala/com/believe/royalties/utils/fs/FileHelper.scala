package com.believe.royalties.utils.fs

import java.io.InputStream
import scala.util.Try

object FileHelper {

  def loadResource(filePath: String, classLoader: ClassLoader): Either[Throwable, InputStream] = {
    Try {
      classLoader.getResourceAsStream(filePath)
    }.toEither
  }
}
