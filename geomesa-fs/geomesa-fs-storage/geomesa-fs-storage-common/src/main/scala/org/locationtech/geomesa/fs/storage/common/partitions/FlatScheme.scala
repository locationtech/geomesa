/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionFilter
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}

object FlatScheme extends PartitionScheme {

  override val name: String = "flat"

  override def getPartition(feature: SimpleFeature): String = ""

  override def getIntersectingPartitions(filter: Filter): Option[Seq[PartitionFilter]] = None

  override def getCoveringFilter(partition: String): Filter = Filter.INCLUDE

  class FlatPartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] =
      if (scheme.equalsIgnoreCase("flat")) { Some(FlatScheme) } else { None }
  }
}
