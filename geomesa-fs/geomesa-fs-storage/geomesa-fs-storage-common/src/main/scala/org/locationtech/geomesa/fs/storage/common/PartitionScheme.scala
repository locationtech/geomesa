/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util.{Collections, ServiceLoader}

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object PartitionScheme {

  // must begin with geomesa in order to be persisted
  val PartitionSchemeKey = "geomesa.fs.partition-scheme.config"

  /**
    * Add the scheme to the sft user data
    *
    * @param sft simple feature type
    * @param scheme partition scheme
    */
  def addToSft(sft: SimpleFeatureType, scheme: PartitionScheme): Unit =
    sft.getUserData.put(PartitionSchemeKey, toConfig(scheme).root().render(ConfigRenderOptions.concise))

  /**
    * Parse a scheme out of the sft user data
    *
    * @param sft simple feature type
    * @return partition scheme, if present
    */
  def extractFromSft(sft: SimpleFeatureType): Option[PartitionScheme] = {
    Option(sft.getUserData.get(PartitionSchemeKey).asInstanceOf[String])
        .map(ConfigFactory.parseString)
        .map(apply(sft, _))
  }

  /**
    * Load a matching partition scheme via SPI lookup
    *
    * @param sft simple feature type
    * @param name scheme name
    * @param opts scheme options
    * @return
    */
  def apply(sft: SimpleFeatureType,
            name: String,
            opts: java.util.Map[String, String] = Collections.emptyMap()): PartitionScheme = {
    import org.locationtech.geomesa.utils.conversions.JavaConverters._
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits._

    import scala.collection.JavaConversions._

    ServiceLoader.load(classOf[PartitionSchemeFactory])
        .iterator()
        .flatMap(_.load(name, sft, opts).asScala.toSeq)
        .headOption
        .getOrElse(throw new IllegalArgumentException(s"Could not load partition scheme from '$name' - $opts"))
  }

  /**
    * Load a matching partition scheme via SPI lookup
    *
    * @param sft simple feature type
    * @param conf typesafe config
    * @return
    */
  def apply(sft: SimpleFeatureType, conf: Config): PartitionScheme = {
    require(conf.hasPath("scheme"), "config must have a scheme")
    require(conf.hasPath("options"), "config must have options for scheme")

    val schemeName = conf.getString("scheme")
    val optConf = conf.getConfig("options")
    val opts = optConf.entrySet().map(e => e.getKey -> optConf.getString(e.getKey)).toMap

    apply(sft, schemeName, opts)
  }

  /**
    * Serialize a partition scheme as a typesafe config
    *
    * @param scheme partition scheme
    * @return
    */
  def toConfig(scheme: PartitionScheme): Config =
    ConfigFactory.empty()
      .withValue("scheme", ConfigValueFactory.fromAnyRef(scheme.getName))
      .withValue("options", ConfigValueFactory.fromMap(scheme.getOptions))
}
