/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute.legacy

import org.locationtech.geomesa.index.api.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.LegacyTableNaming
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV6.AttributeIndexKeySpaceV6
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV7.AttributeIndexKeySpaceV7
import org.locationtech.geomesa.index.index.attribute.{AttributeIndexKey, AttributeIndexKeySpace, AttributeIndexValues}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Attribute index with secondary z-curve indexing. Z-indexing is based on the sft and will be
  * one of Z3, XZ3, Z2, XZ2. Shards come after the attribute number, instead of before it.
  */
class AttributeIndexV6 protected (ds: GeoMesaDataStore[_],
                                  sft: SimpleFeatureType,
                                  version: Int,
                                  attribute: String,
                                  secondaries: Seq[String],
                                  mode: IndexMode)
    extends AttributeIndexV7(ds, sft, version, attribute, secondaries, mode)
        with LegacyTableNaming[AttributeIndexValues[Any], AttributeIndexKey] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, attribute: String, secondaries: Seq[String], mode: IndexMode) =
    this(ds, sft, 6, attribute, secondaries, mode)

  override val keySpace: AttributeIndexKeySpace = {
    val sharding = AttributeShardStrategy(sft)
    if (sharding.shards.nonEmpty) {
      // if sharding, we need to swap the shard bytes with the idx bytes
      new AttributeIndexKeySpaceV6(sft, sft.getTableSharingBytes, sharding, attribute)
    } else {
      // otherwise we can skip the swap and use the parent class
      new AttributeIndexKeySpaceV7(sft, sft.getTableSharingBytes, sharding, attribute)
    }
  }

  override protected val fallbackTableNameKey: String = "tables.idx.attr.name"
}

object AttributeIndexV6 {

  /**
    * Map from (sharing, shard, idx0, idx1) to (sharing, idx0, idx1, shard)
    *
    * Rows in the attribute table have the following layout:
    *
    * - 1 byte identifying the sft (OPTIONAL - only if table is shared)
    * - 2 bytes storing the index of the attribute in the sft
    * - 1 byte shard (OPTIONAL)
    * - n bytes storing the lexicoded attribute value
    * - NULLBYTE as a separator
    * - n bytes storing the secondary z-index of the feature - identified by getSecondaryIndexKeyLength
    * - n bytes storing the feature ID
    *
    */
  class AttributeIndexKeySpaceV6(sft: SimpleFeatureType,
                                 sharing: Array[Byte],
                                 sharding: ShardStrategy,
                                 attributeField: String)
      extends AttributeIndexKeySpaceV7(sft, sharing, sharding, attributeField) {

    private val offset = sharing.length

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[AttributeIndexKey] = {
      val key = super.toIndexKey(writable, tier, id, lenient)
      key match {
        case kv: SingleRowKeyValue[AttributeIndexKey] => swap(kv.row)
        case kv: MultiRowKeyValue[AttributeIndexKey] => kv.rows.foreach(swap)
      }
      key
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]], tier: Boolean): Iterator[ByteRange] = {
      super.getRangeBytes(ranges, tier).map {
        case BoundedByteRange(lower, upper)      => BoundedByteRange(swap(lower), swap(upper))
        case SingleRowByteRange(row)             => SingleRowByteRange(swap(row))
        case LowerBoundedByteRange(lower, upper) => LowerBoundedByteRange(swap(lower), swap(upper))
        case UpperBoundedByteRange(lower, upper) => UpperBoundedByteRange(swap(lower), swap(upper))
        case UnboundedByteRange(lower, upper)    => UnboundedByteRange(swap(lower), swap(upper))
        case r => throw new NotImplementedError(s"Unexpected byte range: $r")
      }
    }

    private def swap(bytes: Array[Byte]): Array[Byte] = {
      if (bytes.length > 0) {
        val shard = bytes(offset)
        bytes(offset) = bytes(offset + 1)
        bytes(offset + 1) = bytes(offset + 2)
        bytes(offset + 2) = shard
      }
      bytes
    }
  }
}
