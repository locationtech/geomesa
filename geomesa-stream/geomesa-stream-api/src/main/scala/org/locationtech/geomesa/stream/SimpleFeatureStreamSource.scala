package org.locationtech.geomesa.stream

import java.util.ServiceLoader

import com.typesafe.config.Config
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait SimpleFeatureStreamSource {
  def next: SimpleFeature
  def sft: SimpleFeatureType
  def init(): Unit = {}
}

trait SimpleFeatureStreamSourceFactory {
  def canProcess(conf: Config): Boolean
  def create(conf: Config): SimpleFeatureStreamSource
}

object SimpleFeatureStreamSource {
  def buildSource(conf: Config): SimpleFeatureStreamSource = {
    import scala.collection.JavaConversions._
    val factory = ServiceLoader.load(classOf[SimpleFeatureStreamSourceFactory]).iterator().find(_.canProcess(conf))
    factory.map { f => f.create(conf) }.getOrElse(throw new RuntimeException("Cannot load source"))
  }
}

