/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

trait ConfigSftParsing extends LazyLogging {

  import scala.collection.JavaConverters._

  /**
    * Parse out simple feature types from a config
    *
    * @param config config
    * @return
    */
  def parseConf(config: Config): Seq[SimpleFeatureType] = {
    if (!config.hasPath(ConfigSftParsing.path)) { Seq.empty } else {
      val confs = config.getConfig(ConfigSftParsing.path)
      confs.root.keySet.asScala.toSeq.flatMap { name =>
        val sftConf = confs.getConfig(name)
        try {
          Some(SimpleFeatureTypes.createType(sftConf, Some(name)))
        } catch {
          case e: Exception =>
            logger.error("Error loading simple feature type from config " +
                s"${sftConf.root().render(ConfigRenderOptions.concise())}", e)
            None
        }
      }
    }
  }
}

object ConfigSftParsing extends ConfigSftParsing {

  val ConfigPathProperty = SystemProperty("org.locationtech.geomesa.sft.config.path", "geomesa.sfts")

  // keep as function so its mutable
  def path: String = ConfigPathProperty.get
}
