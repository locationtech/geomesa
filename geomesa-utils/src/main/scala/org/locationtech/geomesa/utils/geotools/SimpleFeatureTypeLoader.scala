/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.net.URL
import java.util.Collections

import com.typesafe.config.ConfigFactory
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Query all SimpleFeatureTypeProviders to expose all available SimpleFeatureTypes
 * available to the the system. Note that the list of SimpleFeatureTypes
 * available may change over time as SimpleFeatureTypeProviders may dynamically
 * modify their available SFTs.
 */
object SimpleFeatureTypeLoader {

  import scala.collection.JavaConverters._

  private lazy val providers = ServiceLoader.load[SimpleFeatureTypeProvider]()

  // keep as a method so we can dynamically reload
  def sfts: List[SimpleFeatureType] = providers.flatMap(_.loadTypes().asScala)

  // Public API
  def listTypeNames: List[String] = sfts.map(_.getTypeName)
  def sftForName(n: String): Option[SimpleFeatureType] = sfts.find(_.getTypeName == n)

  /**
    * Class path provider
    */
  class ClassPathSftProvider extends SimpleFeatureTypeProvider with ConfigSftParsing {
    override def loadTypes(): java.util.List[SimpleFeatureType] = {
      val sfts = parseConf(ConfigFactory.load())
      logger.debug(s"Loading SFTs from classpath ${sfts.map(_.getTypeName).mkString(", ")}")
      sfts.asJava
    }
  }

  /**
    * Load types from arbitrary urls
    */
  class URLSftProvider extends SimpleFeatureTypeProvider with ConfigSftParsing {
    override def loadTypes(): java.util.List[SimpleFeatureType] = {
      val urls = configURLs.toList
      logger.debug(s"Loading config from urls: ${urls.mkString(", ")}")
      val configs = urls.flatMap { url =>
        logger.debug(s"Attempting to parse config from url $url")
        try {
          Some(ConfigFactory.parseURL(url))
        } catch {
          case e: Throwable =>
            logger.warn(s"Unable to load SFT config from url $url")
            logger.trace(s"Unable to load SFT config from url $url", e)
            None
        }
      }
      configs.reduceLeftOption(_.withFallback(_)) match {
        case Some(c) => parseConf(c).asJava
        case None => Collections.emptyList[SimpleFeatureType]()
      }
    }

    // Will also pick things up from the SystemProperties
    private def configURLs: Seq[URL] = {
      val config = ConfigFactory.load()
      if (!config.hasPath(URLSftProvider.SftConfigURLs)) { Seq.empty[URL] } else {
        config.getAnyRef(URLSftProvider.SftConfigURLs) match {
          case s: String => s.split(',').map(s => new URL(s.trim)).toList
          case s: java.util.List[String] => s.asScala.map(new URL(_))
        }
      }
    }
  }

  object URLSftProvider {
    val SftConfigURLs = "geomesa.sft.config.urls"
  }
}
