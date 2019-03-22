/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api


import java.util.ServiceLoader

import org.opengis.feature.simple.SimpleFeatureType

/**
  * Factory for loading partition schemes
  */
trait PartitionSchemeFactory {

  /**
    * Load a partition scheme
    *
    * @param sft simple feature type
    * @param config scheme options
    * @return partition scheme
    */
  def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme]
}

object PartitionSchemeFactory {

  import scala.collection.JavaConverters._

  lazy private val factories = ServiceLoader.load(classOf[PartitionSchemeFactory]).asScala.toSeq

  /**
    * Create a partition scheme instance via SPI lookup
    *
    * @param sft simple feature type
    * @param config scheme options
    * @return
    */
  def load(sft: SimpleFeatureType, config: NamedOptions): PartitionScheme = {
    factories.toStream.flatMap(_.load(sft, config)).headOption.getOrElse {
      throw new IllegalArgumentException(s"No partition scheme factory implementation exists for name " +
          s"'${config.name}'. Available factories: ${factories.map(_.getClass.getName).mkString(", ")}")
    }
  }
}
