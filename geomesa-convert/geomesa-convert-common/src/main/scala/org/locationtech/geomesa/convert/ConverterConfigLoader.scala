/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import java.net.URL
import java.util
import java.util.{List => JList}
import javax.imageio.spi.ServiceRegistry

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


object ConverterConfigLoader {

  val ConfigPathProperty: String = "org.locationtech.geomesa.converter.config.path"
  private val configProviders = ServiceRegistry.lookupProviders(classOf[ConverterConfigProvider]).toList

  def path: String = sys.props.getOrElse(ConfigPathProperty, "geomesa.converters")

  // this is intentionally a method to allow reloading by the providers
  def confs: Map[String, Config] = configProviders.map(_.loadConfigs).reduce( _ ++ _).toMap

  // Public API
  def listConverterNames: List[String] = confs.keys.toList
  def getAllConfigs: Map[String, Config] = confs
  def configForName(name: String) = confs.get(name)

  // Rebase a config to to the converter root...allows standalone
  // configurations to start with "converter", "input-converter"
  // or optional other prefix string
  def rebaseConfig(conf: Config, path: Option[String] = None): Config = {
    import org.locationtech.geomesa.utils.conf.ConfConversions._
    (path.toSeq ++ Seq("converter", "input-converter"))
      .foldLeft(conf)( (c, p) => c.getConfigOpt(p).map(c.withFallback).getOrElse(c))
  }

}

trait GeoMesaConvertParser extends LazyLogging {
  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  protected val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)

  def parseConf(config: Config): Map[String, Config] = {
    import scala.collection.JavaConversions._
    logger.trace(s"Attempting to load Converters from path ${ConverterConfigLoader.path}")

    if (!config.hasPath(ConverterConfigLoader.path)) {
      Map.empty[String, Config]
    } else {
      val confs = config.getConfig(ConverterConfigLoader.path)
      confs.root.keySet.map { k =>
        logger.trace(s"Found conf block $k")
        k -> confs.getConfig(k)
      }.toMap[String, Config]
    }
  }
}

/**
  * Provides access converter configs on the classpath
  */
class ClassPathConfigProvider extends ConverterConfigProvider with GeoMesaConvertParser {
  lazy val confs: java.util.Map[String, Config] = parseConf(ConfigFactory.load)
  override def loadConfigs(): util.Map[String, Config] = confs
}

/** Load Config from URLs */
class URLConfigProvider extends ConverterConfigProvider with GeoMesaConvertParser {
  import URLConfigProvider._

  override def loadConfigs(): util.Map[String, Config] =
    configURLs
      .map(ConfigFactory.parseURL)
      .reduceLeftOption(_.withFallback(_))
      .map(parseConf)
      .getOrElse(Map.empty[String, Config]).asJava

  // Will also pick things up from the SystemProperties
  def configURLs: Seq[URL] = {
    val config = ConfigFactory.load(parseOpts)
    if (config.hasPath(ConverterConfigURLs)) {
      config.getAnyRef(ConverterConfigURLs) match {
        case s: String          => s.split(',').map(_.trim).map(new URL(_))
        case lst: JList[String] => lst.map(new URL(_))
      }
    } else {
      Seq.empty[URL]
    }
  }

}

object URLConfigProvider {
  val ConverterConfigURLs = "geomesa.convert.config.urls"
}
