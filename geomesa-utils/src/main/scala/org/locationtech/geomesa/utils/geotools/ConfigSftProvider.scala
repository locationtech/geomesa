/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.geotools

import javax.imageio.spi.ServiceRegistry

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.slf4j.Logging
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Provides simple feature types based on configs on the classpath
 */
class ConfigSftProvider extends SimpleFeatureTypeProvider with Logging {
  override def loadTypes(): java.util.List[SimpleFeatureType] = {
    val config = ConfigFactory.load()
    val path = sys.props.getOrElse(ConfigSftProvider.ConfigPathProperty, "geomesa.sfts")
    if (!config.hasPath(path)) {
      return List.empty[SimpleFeatureType]
    }
    config.getConfigList(path).flatMap { sft =>
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

object ConfigSftProvider {
  val ConfigPathProperty = "org.locationtech.geomesa.sft.config.path"
}

object SimpleFeatureTypeLoader {
  val sfts: List[SimpleFeatureType] =
    ServiceRegistry.lookupProviders(classOf[SimpleFeatureTypeProvider]).flatMap(_.loadTypes()).toList
}
