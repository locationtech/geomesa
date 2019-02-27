/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class IdIndexKeySpace(val sft: SimpleFeatureType) extends IndexKeySpace[Set[Array[Byte]], Array[Byte]] {

  import IdIndexKeySpace.Empty

  private val idToBytes = GeoMesaFeatureIndex.idToBytes(sft)

  override val attributes: Seq[String] = Seq.empty

  // note: technically this doesn't match the index key, but it's only
  // used for extracting the feature ID so it works out
  override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(0)

  override val sharing: Array[Byte] = Empty

  override val sharding: ShardStrategy = NoShardStrategy

  override def toIndexKey(writable: WritableFeature,
                          tier: Array[Byte],
                          id: Array[Byte],
                          lenient: Boolean): RowKeyValue[Array[Byte]] = {
    SingleRowKeyValue(id, Empty, Empty, id, tier, id, writable.values)
  }

  override def getIndexValues(filter: Filter, explain: Explainer): Set[Array[Byte]] = {
    // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
    // intersect together all groups of ID Filters, producing a set of IDs
    val identifiers = IdFilterStrategy.intersectIdFilters(filter)
    explain(s"Extracted ID filter: ${identifiers.mkString(", ")}")
    identifiers.map(idToBytes.apply)
  }

  override def getRanges(values: Set[Array[Byte]], multiplier: Int): Iterator[ScanRange[Array[Byte]]] =
    values.iterator.map(SingleRowRange.apply)

  override def getRangeBytes(ranges: Iterator[ScanRange[Array[Byte]]], tier: Boolean): Iterator[ByteRange] = {
    if (sharding.length == 0) {
      ranges.map {
        case SingleRowRange(row) => SingleRowByteRange(row)
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case SingleRowRange(row) => sharding.shards.map(p => SingleRowByteRange(ByteArrays.concat(p, row)))
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  override def useFullFilter(values: Option[Set[Array[Byte]]],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = false
}

object IdIndexKeySpace extends IndexKeySpaceFactory[Set[Array[Byte]], Array[Byte]] {

  private val Empty = Array.empty[Byte]

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean = attributes.isEmpty

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): IdIndexKeySpace =
    new IdIndexKeySpace(sft)
}
