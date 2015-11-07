package org.locationtech.geomesa.blob.core.handlers

import java.io.File

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.util.{Z3UuidGenerator, Z3FeatureIdGenerator}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._

object BlobStoreFileHander {

  def buildSF(file: File, params: Map[String, String]): SimpleFeature = new WKTFileHander().buildSF(file, params)

}

trait BlobStoreFileHander {
  def canProcess(file: File, params: Map[String, String]): Boolean

  def buildSF(file: File, params: Map[String, String]): SimpleFeature
}

class WKTFileHander extends BlobStoreFileHander {
  val builder = new SimpleFeatureBuilder(sft)
  val featureIdGenerator = new Z3FeatureIdGenerator

  override def canProcess(file: File, params: Map[String, String]): Boolean = ???

  override def buildSF(file: File, params: Map[String, String]): SimpleFeature = {
    val wkt: Geometry = WKTUtils.read(params("wkt"))

    val z3id = Z3UuidGenerator.createUuid(wkt, System.currentTimeMillis())

    builder.set("geom", wkt)
    builder.set(idFieldName, z3id)

    builder.buildFeature("")
  }
}