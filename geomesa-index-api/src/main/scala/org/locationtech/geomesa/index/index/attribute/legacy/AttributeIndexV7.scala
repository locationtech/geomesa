/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute.legacy

import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.index.api.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV7.AttributeIndexKeySpaceV7
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexKeySpace}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.ByteArrays.{OneByteArray, ZeroByteArray}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

/**
  * Attribute index with tiering based on the default date and geom. All attributes share a single table
  *
  * @param ds data store
  * @param sft simple feature type stored in this index
  * @param version version of the index
  * @param attribute attribute field being indexed
  * @param secondaries secondary fields used for the index tiering
  * @param mode mode of the index (read/write/both)
  */
class AttributeIndexV7 protected (ds: GeoMesaDataStore[_],
                                  sft: SimpleFeatureType,
                                  version: Int,
                                  attribute: String,
                                  secondaries: Seq[String],
                                  mode: IndexMode)
    extends AttributeIndex(ds, sft, version, attribute, secondaries, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, attribute: String, secondaries: Seq[String], mode: IndexMode) =
    this(ds, sft, 7, attribute, secondaries, mode)

  override protected val tableNameKey: String = s"table.attr.v$version"

  override val keySpace: AttributeIndexKeySpace =
    new AttributeIndexKeySpaceV7(sft, sft.getTableSharingBytes, AttributeShardStrategy(sft), attribute)
}

object AttributeIndexV7 {

  class AttributeIndexKeySpaceV7(sft: SimpleFeatureType,
                                 override val sharing: Array[Byte],
                                 sharding: ShardStrategy,
                                 attributeField: String)
      extends AttributeIndexKeySpace(sft, sharding, attributeField) {

    private val idxBytes = AttributeIndexKey.indexToBytes(fieldIndex)

    private val rangePrefixes = {
      if (sharing.isEmpty) {
        sharding.shards
      } else if (sharding.length == 0) {
        Seq(sharing)
      } else {
        sharding.shards.map(ByteArrays.concat(sharing, _))
      }
    }

    override val indexKeyByteLength: Left[(Array[Byte], Int, Int) => Int, Int] =
      Left((row, offset, _) =>
        row.indexOf(ByteArrays.ZeroByte, offset + sharding.length + sharing.length + 2) + 1 - offset)

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[AttributeIndexKey] = {
      val shard = sharding(writable)

      if (isList) {
        val attribute = writable.getAttribute[java.util.List[Any]](fieldIndex)
        if (attribute == null) {
          MultiRowKeyValue(Seq.empty, sharing, shard, Seq.empty, tier, id, writable.values)
        } else {
          val rows = Seq.newBuilder[Array[Byte]]
          val keys = Seq.newBuilder[AttributeIndexKey]

          var j = 0
          while (j < attribute.size()) {
            val encoded = AttributeIndexKey.typeEncode(attribute.get(j))
            val value = encoded.getBytes(StandardCharsets.UTF_8)

            // create the byte array - allocate a single array up front to contain everything
            // +2 for idx bytes, +1 for null byte
            val bytes = Array.ofDim[Byte](sharing.length + shard.length + tier.length + id.length + 3 + value.length)
            var i = 0
            if (!sharing.isEmpty) {
              bytes(0) = sharing.head // sharing is only a single byte
              i += 1
            }
            if (!shard.isEmpty) {
              bytes(i) = shard.head // shard is only a single byte
              i += 1
            }
            bytes(i) = idxBytes(0)
            bytes(i + 1) = idxBytes(1)
            i += 2
            System.arraycopy(value, 0, bytes, i, value.length)
            i += value.length
            bytes(i) = ByteArrays.ZeroByte
            System.arraycopy(tier, 0, bytes, i + 1, tier.length)
            System.arraycopy(id, 0, bytes, i + 1 + tier.length, id.length)

            rows += bytes
            keys += AttributeIndexKey(fieldIndexShort, encoded)

            j += 1
          }
          MultiRowKeyValue(rows.result, sharing, shard, keys.result, tier, id, writable.values)
        }
      } else {
        val attribute = writable.getAttribute[Any](fieldIndex)
        if (attribute == null) {
          MultiRowKeyValue(Seq.empty, sharing, shard, Seq.empty, tier, id, writable.values)
        } else {
          val encoded = AttributeIndexKey.typeEncode(attribute)
          val value = encoded.getBytes(StandardCharsets.UTF_8)

          // create the byte array - allocate a single array up front to contain everything
          // +2 for idx bytes, +1 for null byte
          val bytes = Array.ofDim[Byte](sharing.length + shard.length + tier.length + id.length + 3 + value.length)
          var i = 0
          if (!sharing.isEmpty) {
            bytes(0) = sharing.head // sharing is only a single byte
            i += 1
          }
          if (!shard.isEmpty) {
            bytes(i) = shard.head // shard is only a single byte
            i += 1
          }
          bytes(i) = idxBytes(0)
          bytes(i + 1) = idxBytes(1)
          i += 2
          System.arraycopy(value, 0, bytes, i, value.length)
          i += value.length
          bytes(i) = ByteArrays.ZeroByte
          System.arraycopy(tier, 0, bytes, i + 1, tier.length)
          System.arraycopy(id, 0, bytes, i + 1 + tier.length, id.length)

          SingleRowKeyValue(bytes, sharing, shard, AttributeIndexKey(fieldIndexShort, encoded), tier, id, writable.values)
        }
      }
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]], tier: Boolean): Iterator[ByteRange] = {
      if (tier) {
        getTieredRangeBytes(ranges, rangePrefixes)
      } else {
        getStandardRangeBytes(ranges, rangePrefixes)
      }
    }

    override def decodeRowValue(row: Array[Byte], offset: Int, length: Int): Try[AnyRef] = Try {
      // start of the encoded value
      // exclude feature byte and 2 index bytes and shard bytes
      val valueStart = offset + sharding.length + sharing.length + 2
      // null byte indicates end of value
      val valueEnd = math.min(row.indexOf(ByteArrays.ZeroByte, valueStart), offset + length)
      decodeValue(new String(row, valueStart, valueEnd - valueStart, StandardCharsets.UTF_8))
    }

    override protected def lower(key: AttributeIndexKey, prefix: Boolean = false): Array[Byte] = {
      if (key.value == null) {
        idxBytes
      } else if (prefix) {
        // note: inclusive doesn't make sense for prefix ranges
        ByteArrays.concat(idxBytes, key.value.getBytes(StandardCharsets.UTF_8))
      } else if (key.inclusive) {
        ByteArrays.concat(idxBytes, key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray)
      } else {
        ByteArrays.concat(idxBytes, key.value.getBytes(StandardCharsets.UTF_8), OneByteArray)
      }
    }

    override protected def upper(key: AttributeIndexKey, prefix: Boolean = false): Array[Byte] = {
      if (key.value == null) {
        ByteArrays.rowFollowingPrefix(idxBytes)
      } else if (prefix) {
        // get the row following the prefix, then get the next row
        // note: inclusiveness doesn't really make sense for prefix ranges
        ByteArrays.rowFollowingPrefix(ByteArrays.concat(idxBytes, key.value.getBytes(StandardCharsets.UTF_8)))
      } else if (key.inclusive) {
        // row following prefix, after the delimiter
        ByteArrays.concat(idxBytes, key.value.getBytes(StandardCharsets.UTF_8), OneByteArray)
      } else {
        // exclude the row
        ByteArrays.concat(idxBytes, key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray)
      }
    }

    override protected def tieredUpper(key: AttributeIndexKey): Option[Array[Byte]] = {
      // note: we can't tier exclusive end points, as we can't calculate previous rows
      if (key.value == null || !key.inclusive) { None } else {
        // match the final row, and count on remaining range to exclude the rest
        Some(ByteArrays.concat(idxBytes, key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray))
      }
    }
  }
}
