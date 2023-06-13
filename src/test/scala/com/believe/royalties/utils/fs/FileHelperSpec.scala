package com.believe.royalties.utils.fs

import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar

import java.io.InputStream

class FileHelperSpec extends AnyWordSpec with Matchers with MockitoSugar {

  "loadResource" should {
    "return Right[InputStream] when file exists" in {
      val classLoader = mock[ClassLoader]
      val inputStream = mock[InputStream]
      when(classLoader.getResourceAsStream("existingFile.txt")).thenReturn(inputStream)

      val result = FileHelper.loadResource("existingFile.txt", classLoader)

      result should matchPattern {
        case Right(`inputStream`) =>
      }
    }

    "return Left[Throwable] when file does not exist" in {
      val classLoader = mock[ClassLoader]
      when(classLoader.getResourceAsStream("nonExistentFile.txt")).thenThrow(new RuntimeException)

      val result = FileHelper.loadResource("nonExistentFile.txt", classLoader)

      result should matchPattern {
        case Left(_: Throwable) =>
      }
    }
  }
}
