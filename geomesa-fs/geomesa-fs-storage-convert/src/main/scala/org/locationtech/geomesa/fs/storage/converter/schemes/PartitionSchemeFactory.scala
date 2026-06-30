/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.schemes

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.converter.schemes.AttributeScheme.AttributePartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.converter.schemes.CompositeScheme.CompositePartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.converter.schemes.DateTimeScheme.DateTimePartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.converter.schemes.FlatScheme.FlatPartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.converter.schemes.ReceiptTimeScheme.ReceiptTimePartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.converter.schemes.XZ2Scheme.XZ2PartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.converter.schemes.Z2Scheme.Z2PartitionSchemeFactory

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

  val factories: Seq[PartitionSchemeFactory] = Seq(
    AttributePartitionSchemeFactory,
    CompositePartitionSchemeFactory,
    DateTimePartitionSchemeFactory,
    FlatPartitionSchemeFactory,
    ReceiptTimePartitionSchemeFactory,
    XZ2PartitionSchemeFactory,
    Z2PartitionSchemeFactory,
  )

  def load(sft: SimpleFeatureType, config: NamedOptions): PartitionScheme =
    factories.toStream.flatMap(_.load(sft, config)).headOption.getOrElse {
      throw new IllegalArgumentException(s"Could not load a partition scheme from: $config")
    }
}
