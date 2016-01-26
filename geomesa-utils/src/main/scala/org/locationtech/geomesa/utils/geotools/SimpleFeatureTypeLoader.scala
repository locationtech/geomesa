/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.net.URL
import java.util
import javax.imageio.spi.ServiceRegistry

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
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

  private val providers = ServiceRegistry.lookupProviders(classOf[SimpleFeatureTypeProvider])

  // keep as a method so we can dynamically reload
  def sfts: List[SimpleFeatureType] = providers.flatMap(_.loadTypes()).toList

  // Public API
  def listTypeNames: List[String] = sfts.map(_.getTypeName)
  def sftForName(n: String): Option[SimpleFeatureType] = sfts.find(_.getTypeName == n)
}

trait ConfigSftParsing extends LazyLogging {
  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)

  def parseConf(config: Config): java.util.List[SimpleFeatureType] = {
    if (!config.hasPath(ConfigSftParsing.path)) {
      return List.empty[SimpleFeatureType]
    }
    config.getConfigList(ConfigSftParsing.path).flatMap { sft =>
      try {
        Some(SimpleFeatureTypes.createType(sft, None))
      } catch {
        case e: Exception =>
          logger.error("Error loading simple feature type from config " +
            s"${sft.root().render(ConfigRenderOptions.concise())}", e)
          None
      }
    }.asJava
  }
}

object ConfigSftParsing {
  val ConfigPathProperty = "org.locationtech.geomesa.sft.config.path"

  // keep as function so its mutable
  def path = sys.props.getOrElse(ConfigPathProperty, "geomesa.sfts")
}

class ClassPathSftProvider extends SimpleFeatureTypeProvider with ConfigSftParsing {
  override def loadTypes(): java.util.List[SimpleFeatureType] = parseConf(ConfigFactory.load())
}

class URLSftProvider extends SimpleFeatureTypeProvider with ConfigSftParsing {
  override def loadTypes(): util.List[SimpleFeatureType] = {
    configURLs
      .map(ConfigFactory.parseURL)
      .reduceLeftOption(_.withFallback(_))
      .map(parseConf)
      .getOrElse(List.empty[SimpleFeatureType])
  }
  
  // Will also pick things up from the SystemProperties
  def configURLs: Seq[URL] = {
    val config = ConfigFactory.load(parseOpts)
    if (config.hasPath(URLSftProvider.SftConfigURLs)) {
      config.getAnyRef(URLSftProvider.SftConfigURLs) match {
        case s:String => s.split(',').map(s => s.trim).toList.map(new URL(_))
        case strList if classOf[java.util.List[String]].isAssignableFrom(strList.getClass) =>
          strList.asInstanceOf[java.util.List[String]].map(new URL(_))
      }
    } else {
      Seq.empty[URL]
    }
  }
}

object URLSftProvider {
  val SftConfigURLs = "geomesa.sft.config.urls"
}