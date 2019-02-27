/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2.legacy

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.LegacyTableNaming
import org.locationtech.geomesa.index.index.z2.legacy.XZ2IndexV1.XZ2IndexKeySpaceV1
import org.locationtech.geomesa.index.index.z2.{XZ2Index, XZ2IndexKeySpace, XZ2IndexValues}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

// differentiates before adoption of common column families
class XZ2IndexV1(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode)
    extends XZ2Index(ds, sft, 1, geom, mode) with LegacyTableNaming[XZ2IndexValues, Long] {

  override val keySpace: XZ2IndexKeySpace =
    new XZ2IndexKeySpaceV1(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom)

  override protected val tableNameKey: String = "table.xz2.v1"
}

object XZ2IndexV1 {

  class XZ2IndexKeySpaceV1(sft: SimpleFeatureType,
                           override val sharing: Array[Byte],
                           sharding: ShardStrategy,
                           geomField: String) extends XZ2IndexKeySpace(sft, sharding, geomField) {

    private val rangePrefixes = {
      if (sharding.length == 0 && sharing.isEmpty) {
        Seq.empty
      } else if (sharing.isEmpty) {
        sharding.shards
      } else if (sharding.length == 0) {
        Seq(sharing)
      } else {
        sharding.shards.map(ByteArrays.concat(sharing, _))
      }
    }

    override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] =
      Right(8 + sharing.length + sharding.length)

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Long] = {
      val geom = writable.getAttribute[Geometry](geomIndex)
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
      }
      val envelope = geom.getEnvelopeInternal
      val xz = try { sfc.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY, lenient) } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid xz value from geometry: $geom", e)
      }
      val shard = sharding(writable)

      // create the byte array - allocate a single array up front to contain everything
      // ignore tier, not used here
      val bytes = Array.ofDim[Byte](sharing.length + shard.length + 8 + id.length)
      var i = 0
      if (!sharing.isEmpty) {
        bytes(0) = sharing.head // sharing is only a single byte
        i += 1
      }
      if (!shard.isEmpty) {
        bytes(i) = shard.head // shard is only a single byte
        i += 1
      }
      ByteArrays.writeLong(xz, bytes, i)
      System.arraycopy(id, 0, bytes, i + 8, id.length)

      SingleRowKeyValue(bytes, sharing, shard, xz, tier, id, writable.values)
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[Long]], tier: Boolean): Iterator[ByteRange] = {
      if (rangePrefixes.isEmpty) {
        ranges.map {
          case BoundedRange(lo, hi) => BoundedByteRange(ByteArrays.toBytes(lo), ByteArrays.toBytesFollowingPrefix(hi))
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      } else {
        ranges.flatMap {
          case BoundedRange(lo, hi) =>
            val lower = ByteArrays.toBytes(lo)
            val upper = ByteArrays.toBytesFollowingPrefix(hi)
            rangePrefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      }
    }
  }
}
