/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.net.URL
import java.util.{ServiceLoader, List => JList}

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
 * Query all SimpleFeatureTypeProviders to expose all available SimpleFeatureTypes
 * available to the the system. Note that the list of SimpleFeatureTypes
 * available may change over time as SimpleFeatureTypeProviders may dynamically
 * modify their available SFTs.
 */
object SimpleFeatureTypeLoader {

  private val providers = ServiceLoader.load(classOf[SimpleFeatureTypeProvider]).toList

  // keep as a method so we can dynamically reload
  def sfts: List[SimpleFeatureType] = providers.flatMap(_.loadTypes())

  // Public API
  def listTypeNames: List[String] = sfts.map(_.getTypeName)
  def sftForName(n: String): Option[SimpleFeatureType] = sfts.find(_.getTypeName == n)
}

trait ConfigSftParsing extends LazyLogging {

  def parseConf(config: Config): java.util.List[SimpleFeatureType] = {
    import scala.collection.JavaConversions._
    if (!config.hasPath(ConfigSftParsing.path)) {
      List.empty[SimpleFeatureType]
    } else {
      val confs = config.getConfig(ConfigSftParsing.path)
      confs.root.keySet.flatMap { name =>
        val sftConf = confs.getConfig(name)
        try {
          val sft = SimpleFeatureTypes.createType(sftConf, Some(name))
          Some(sft)
        } catch {
          case e: Exception =>
            logger.error("Error loading simple feature type from config " +
              s"${sftConf.root().render(ConfigRenderOptions.concise())}", e)
            None
        }
      }.toList.asJava
    }
  }
}

object ConfigSftParsing {
  val ConfigPathProperty = SystemProperty("org.locationtech.geomesa.sft.config.path", "geomesa.sfts")

  // keep as function so its mutable
  def path: String = ConfigPathProperty.get
}

class ClassPathSftProvider extends SimpleFeatureTypeProvider with ConfigSftParsing {
  override def loadTypes(): JList[SimpleFeatureType] = {
    val sfts = parseConf(ConfigFactory.load())
    logger.debug(s"Loading SFTs from classpath ${sfts.map(_.getTypeName).mkString(", ")}")
    sfts
  }
}

class URLSftProvider extends SimpleFeatureTypeProvider with ConfigSftParsing {
  import URLSftProvider._
  override def loadTypes(): JList[SimpleFeatureType] = {
    val urls = configURLs.toList
    logger.debug(s"Loading config from urls: ${urls.mkString(", ")}")
    urls.flatMap { url =>
      logger.debug(s"Attempting to parse config from url $url")
      try {
        Some(ConfigFactory.parseURL(url))
      } catch {
        case e: Throwable =>
          logger.warn(s"Unable to load SFT config from url $url")
          logger.trace(s"Unable to load SFT config from url $url", e)
          None
      }
    }.reduceLeftOption(_.withFallback(_))
    .map(parseConf)
    .getOrElse(List.empty[SimpleFeatureType])
  }
  
  // Will also pick things up from the SystemProperties
  def configURLs: Seq[URL] = {
    val config = ConfigFactory.load()
    if (config.hasPath(SftConfigURLs)) {
      config.getAnyRef(SftConfigURLs) match {
        case s: String          => s.split(',').map(s => s.trim).toList.map(new URL(_))
        case lst: JList[String] => lst.map(new URL(_))
      }
    } else {
      Seq.empty[URL]
    }
  }
}

object URLSftProvider {
  val SftConfigURLs = "geomesa.sft.config.urls"
}

object SimpleSftParser extends ConfigSftParsing {}