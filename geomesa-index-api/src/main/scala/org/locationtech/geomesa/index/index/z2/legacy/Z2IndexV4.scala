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
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV4.Z2IndexKeySpaceV4
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexKeySpace}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.jts.geom.Point
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

// supports table sharing
class Z2IndexV4 protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, geom: String, mode: IndexMode)
    extends Z2Index(ds, sft, version, geom, mode) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode) =
    this(ds, sft, 4, geom, mode)

  override protected val tableNameKey: String = s"table.z2.v$version"

  override val keySpace: Z2IndexKeySpace =
    new Z2IndexKeySpaceV4(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom)
}

object Z2IndexV4 {

  class Z2IndexKeySpaceV4(sft: SimpleFeatureType,
                          override val sharing: Array[Byte],
                          sharding: ShardStrategy,
                          geomField: String) extends Z2IndexKeySpace(sft, sharding, geomField) {

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
      val geom = writable.getAttribute[Point](geomIndex)
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
      }
      val z = try { sfc.index(geom.getX, geom.getY, lenient).z } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry: $geom", e)
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
      ByteArrays.writeLong(z, bytes, i)
      System.arraycopy(id, 0, bytes, i + 8, id.length)

      SingleRowKeyValue(bytes, sharing, shard, z, tier, id, writable.values)
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
