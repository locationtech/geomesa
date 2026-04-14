/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.partitions

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.partitions.schemes.{AttributeScheme, DateTimeScheme, FlatScheme, HierarchicalDateTimeScheme, SpatialScheme, XZ2Scheme, Z2Scheme}

import java.util.ServiceLoader

/**
  * Factory for loading partition schemes
  */
trait PartitionSchemeFactory {

  /**
    * Load a partition scheme
    *
    * @param sft simple feature type
    * @param scheme scheme options
    * @return partition scheme
    */
  def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme]
}

object PartitionSchemeFactory {

  import scala.collection.JavaConverters._

  lazy private val factories = Seq(AttributeScheme, DateTimeScheme, FlatScheme, XZ2Scheme, Z2Scheme) ++
    ServiceLoader.load(classOf[PartitionSchemeFactory]).asScala.toSeq


  /**
    * Create a partition scheme instance via SPI lookup
    *
    * @param sft simple feature type
    * @param scheme scheme options
    * @return
    */
  def load(sft: SimpleFeatureType, scheme: String): PartitionScheme = {
    factories.toStream.flatMap(_.load(sft, scheme)).headOption.getOrElse {
      throw new IllegalArgumentException(s"No partition scheme factory implementation exists for name " +
        s"'$scheme'. Available factories: ${factories.map(_.getClass.getName).mkString(", ")}")
    }
  }
}
