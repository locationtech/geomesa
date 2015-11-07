package org.locationtech.geomesa.blob.core.handlers

import java.io.File
import java.util

import org.locationtech.geomesa.utils.text.WKTUtils

import scala.collection.JavaConversions._

class WKTFileHandler extends AbstractFileHandler {
  override def canProcess(file: File, params: util.Map[String, String]) = {
    params.contains("wkt")
  }

  override def getGeometryFromParams(params: util.Map[String, String]) = {
    Option(WKTUtils.read(params("wkt")))
  }
}