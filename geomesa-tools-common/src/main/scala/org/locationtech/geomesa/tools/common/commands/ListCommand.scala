package org.locationtech.geomesa.tools.common.commands

import com.typesafe.scalalogging.LazyLogging


trait ListCommand extends CommandWithDataStore with LazyLogging {
  val command = "list"
  def execute() = {
    logger.info("Running List Features")
    ds.getTypeNames.foreach(println)
    ds.dispose()
  }
}