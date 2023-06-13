package com.believe.royalties.utils

import org.apache.logging.log4j.{LogManager, Logger}

trait LoggerWrapper {
  val logger: Logger = LogManager.getLogger(getClass)
}
