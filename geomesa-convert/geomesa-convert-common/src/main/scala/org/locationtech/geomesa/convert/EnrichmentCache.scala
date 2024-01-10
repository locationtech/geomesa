/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{Closeable, InputStreamReader}
import java.nio.charset.StandardCharsets

trait EnrichmentCache extends Closeable {
  def get(args: Array[String]): Any
  def put(args: Array[String], value: Any): Unit
  def clear(): Unit
}

trait EnrichmentCacheFactory {
  def canProcess(conf: Config): Boolean
  def build(conf: Config): EnrichmentCache
}

object EnrichmentCache {

  lazy private val factories = ServiceLoader.load[EnrichmentCacheFactory]()

  def apply(conf: Config): EnrichmentCache = {
    factories.find(_.canProcess(conf))
        .getOrElse(throw new RuntimeException(s"Could not find applicable EnrichmentCache for config: $conf"))
        .build(conf)
  }
}

// For testing purposes
class SimpleEnrichmentCache(val cache: java.util.Map[String, java.util.HashMap[String, AnyRef]] = new java.util.HashMap[String, java.util.HashMap[String, AnyRef]]())
    extends EnrichmentCache {

  import scala.collection.JavaConverters._

  override def get(args: Array[String]): Any = Option(cache.get(args(0))).map(_.get(args(1))).orNull

  override def put(args: Array[String], value: Any): Unit =
    cache.asScala.getOrElseUpdate(args(0), new java.util.HashMap[String, AnyRef]()).put(args(1), value.asInstanceOf[AnyRef])

  override def clear(): Unit = cache.clear()

  override def close(): Unit = {}
}

class SimpleEnrichmentCacheFactory extends EnrichmentCacheFactory {
  override def canProcess(conf: Config): Boolean = conf.hasPath("type") && conf.getString("type").equals("simple")

  override def build(conf: Config): EnrichmentCache = new SimpleEnrichmentCache(conf.getConfig("data").root().unwrapped().asInstanceOf[java.util.Map[String, java.util.HashMap[String, AnyRef]]])
}

class ResourceLoadingCache(path: String, idField: String, headers: Seq[String]) extends EnrichmentCache {
  import scala.collection.JavaConverters._

  private val data = {
    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    val stream = loader.getResourceAsStream(path)
    if (stream == null) {
      throw new IllegalArgumentException(s"Could not load the resources at '$path'")
    }
    val reader = new InputStreamReader(stream, StandardCharsets.UTF_8)
    val format = CSVFormat.DEFAULT.withHeader(headers: _*)
    WithClose(new CSVParser(reader, format)) { reader =>
      reader.getRecords.asScala.map(rec => rec.get(idField) -> rec.toMap).toMap
    }
  }

  override def get(args: Array[String]): Any = data.get(args(0)).map(_.get(args(1))).orNull
  override def put(args: Array[String], value: Any): Unit = ???
  override def clear(): Unit = ???
  override def close(): Unit = {}
}

class ResourceLoadingCacheFactory extends EnrichmentCacheFactory {
  override def canProcess(conf: Config): Boolean = conf.hasPath("type") && conf.getString("type").equals("resource")

  override def build(conf: Config): EnrichmentCache = {
    import scala.collection.JavaConverters._

    val path = conf.getString("path")
    val idField = conf.getString("id-field")
    val headers = conf.getStringList("columns")
    new ResourceLoadingCache(path, idField, headers.asScala.toList)
  }
}