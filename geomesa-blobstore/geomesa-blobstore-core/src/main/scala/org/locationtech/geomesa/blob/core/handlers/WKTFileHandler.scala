package org.locationtech.geomesa.blob.core.handlers

import java.io.File
import java.{lang, util}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.accumulo.util.{Z3UuidGenerator, Z3FeatureIdGenerator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

class WKTFileHandler extends FileHandler {
  val builder = new SimpleFeatureBuilder(sft)
  val featureIdGenerator = new Z3FeatureIdGenerator

  override def buildSF(file: File, params: util.Map[String, String]): SimpleFeature = {
    val wkt: Geometry = WKTUtils.read(params("wkt"))

    val z3id = Z3UuidGenerator.createUuid(wkt, System.currentTimeMillis())

    builder.set(geomeFieldName, wkt)
    builder.set(idFieldName, z3id)

    builder.buildFeature("")
  }

  override def canProcess(file: File, params: util.Map[String, String]): lang.Boolean = params.contains("wkt")
}