/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.partitions
package schemes

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.metadata.StorageMetadata.PartitionKey
import org.locationtech.geomesa.fs.storage.core.partitions.PartitionScheme.PartitionRange

object FlatScheme extends PartitionScheme with PartitionSchemeFactory {

  override val name: String = "flat"

  override def getPartition(feature: SimpleFeature): PartitionKey = PartitionKey(name, "")

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = None

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = None

  override def getCoveringFilter(partition: PartitionKey): Filter = Filter.INCLUDE

  override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] =
    if (scheme.equalsIgnoreCase("flat")) { Some(FlatScheme) } else { None }
}
