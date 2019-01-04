/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.sft

import com.typesafe.config._
import org.locationtech.geomesa.utils.geotools.ConfigSftParsing
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.KEYWORDS_KEY
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.KEYWORDS_DELIMITER
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec._
import org.opengis.feature.simple.SimpleFeatureType

/**
  * SimpleFeatureSpec parsing from/to typesafe config
  */
object SimpleFeatureSpecConfig {

  import scala.collection.JavaConverters._

  val TypeNamePath   = "type-name"
  val AttributesPath = "attributes"
  val UserDataPath   = "user-data"

  val TypePath       = "type"
  val NamePath       = "name"

  // config keys that are not attribute options - all other fields are assumed to be options
  private val NonOptions = Seq(TypePath, NamePath)

  /**
    * Parse a SimpleFeatureType spec from a typesafe Config
    *
    * @param conf config
    * @param path instead of parsing the root config, parse the nested config at this path
    * @return
    */
  def parse(conf: Config, path: Option[String]): (Option[String], SimpleFeatureSpec) = {
    import org.locationtech.geomesa.utils.conf.ConfConversions._

    val toParse = path match {
      case Some(p) => conf.getConfigOpt(p).map(conf.withFallback).getOrElse(conf)
      case None    => conf
    }
    parse(toParse)
  }

  /**
    * Convert a simple feature type to a typesafe config
    *
    * @param sft simple feature type
    * @param includeUserData include user data
    * @param includePrefix include the geomesa.sfts.XXX prefix
    * @return
    */
  def toConfig(sft: SimpleFeatureType, includeUserData: Boolean, includePrefix: Boolean): Config = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // Update "default" options (dtg and geom)
    val defaults = sft.getDtgField.toSeq ++ Option(sft.getGeomField)
    val attributes = sft.getAttributeDescriptors.asScala.map { ad =>
      val config = SimpleFeatureSpec.attribute(sft, ad).toConfigMap
      if (defaults.contains(ad.getLocalName)) {
        config.updated("default", "true").asJava
      } else {
        config.asJava
      }
    }

    val base = ConfigFactory.empty()
      .withValue(TypeNamePath, ConfigValueFactory.fromAnyRef(sft.getTypeName))
      .withValue(AttributesPath, ConfigValueFactory.fromIterable(attributes.asJava))

    val updated = if (includeUserData) {
      val prefixes = sft.getUserDataPrefixes
      // special handling for keywords delimiter
      val keywords = Map(KEYWORDS_KEY -> sft.getKeywords.asJava).filterNot(_._2.isEmpty)
      val toConvert = keywords ++ sft.getUserData.asScala.collect {
        case (k, v) if v != null && prefixes.exists(k.toString.startsWith) && k != KEYWORDS_KEY => (k.toString, v)
      }
      val userData = ConfigValueFactory.fromMap(toConvert.asJava)
      base.withValue(UserDataPath, userData)
    } else {
      base
    }

    if (includePrefix) {
      updated.atPath(s"${ConfigSftParsing.path}.${sft.getTypeName}")
    } else {
      updated
    }
  }

  /**
    * Convert a simple feature type to a typesafe config and renders it as a string
    *
    * @param sft simple feature type
    * @param includeUserData include user data
    * @param concise concise or verbose string
    * @return
    */
  def toConfigString(sft: SimpleFeatureType,
                     includeUserData: Boolean,
                     concise: Boolean,
                     includePrefix: Boolean,
                     json: Boolean): String = {
    val opts = if (concise) {
      ConfigRenderOptions.concise.setJson(json)
    } else {
      ConfigRenderOptions.defaults().setFormatted(true).setComments(false).setOriginComments(false).setJson(json)
    }
    toConfig(sft, includeUserData, includePrefix).root().render(opts)
  }

  private def parse(conf: Config): (Option[String], SimpleFeatureSpec) = {
    import org.locationtech.geomesa.utils.conf.ConfConversions._

    val name = conf.getStringOpt(TypeNamePath)
    val attributes = conf.getConfigListOpt("fields").getOrElse(conf.getConfigList(AttributesPath)).asScala.map(buildField)
    val opts = getOptions(conf.getConfigOpt(UserDataPath).getOrElse(ConfigFactory.empty))

    (name, SimpleFeatureSpec(attributes, opts))
  }

  private def buildField(conf: Config): AttributeSpec = {
    val attribute = SimpleFeatureSpecParser.parseAttribute(s"${conf.getString(NamePath)}:${conf.getString(TypePath)}")
    val options = getOptions(conf)

    attribute match {
      case s: SimpleAttributeSpec => s.copy(options = options)
      case s: GeomAttributeSpec   => s.copy(options = options)
      case s: ListAttributeSpec   => s.copy(options = options)
      case s: MapAttributeSpec    => s.copy(options = options)
    }
  }

  def normalizeKey(k: String): String = String.join(".", ConfigUtil.splitPath(k))

  private def getOptions(conf: Config): Map[String, String] = {
    val asMap = conf.entrySet().asScala.map(e => normalizeKey(e.getKey) -> e.getValue.unwrapped()).toMap
    asMap.filterKeys(!NonOptions.contains(_)).map {
      // Special case to handle adding keywords
      case (KEYWORDS_KEY, v: java.util.List[String]) => KEYWORDS_KEY -> String.join(KEYWORDS_DELIMITER, v)
      case (k, v: java.util.List[String]) => k -> String.join(",", v)
      case (k, v) => k -> s"$v"
    }
  }

}
