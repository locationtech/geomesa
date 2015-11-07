package org.locationtech.geomesa.blob.core.handlers

import java.io.File
import javax.imageio.spi.ServiceRegistry

import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object BlobStoreFileHander {
  def buildSF(file: File, params: Map[String, String]): Option[SimpleFeature] = {
    val handlers = ServiceRegistry.lookupProviders(classOf[FileHandler])

    handlers.find(_.canProcess(file, params)).map(_.buildSF(file, params))
  }
}

