/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3.legacy

import java.util.Date

import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV5.Z3IndexKeySpaceV5
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV6.Z3IndexKeySpaceV6
import org.locationtech.geomesa.index.index.z3.{Z3IndexKey, Z3IndexKeySpace}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.jts.geom.Point
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

// fixed z-curve, still has table sharing
class Z3IndexV5 protected (ds: GeoMesaDataStore[_],
                           sft: SimpleFeatureType,
                           version: Int,
                           geom: String,
                           dtg: String,
                           mode: IndexMode) extends Z3IndexV6(ds, sft, version: Int, geom, dtg, mode) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode) =
    this(ds, sft, 5, geom, dtg, mode)

  override protected val tableNameKey: String = s"table.z3.v$version"

  // noinspection ScalaDeprecation
  override val keySpace: Z3IndexKeySpace =
    new Z3IndexKeySpaceV5(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom, dtg)
}

object Z3IndexV5 {

  class Z3IndexKeySpaceV5(sft: SimpleFeatureType,
                          override val sharing: Array[Byte],
                          sharding: ShardStrategy,
                          geomField: String,
                          dtgField: String) extends Z3IndexKeySpaceV6(sft, sharding, geomField, dtgField) {

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
      Right(10 + sharing.length + sharding.length)

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Z3IndexKey] = {
      val geom = writable.getAttribute[Point](geomIndex)
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
      }
      val dtg = writable.getAttribute[Date](dtgIndex)
      val time = if (dtg == null) { 0 } else { dtg.getTime }
      val BinnedTime(b, t) = timeToIndex(time)
      val z = try { sfc.index(geom.getX, geom.getY, t, lenient) } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry/time: $geom,$dtg", e)
      }
      val shard = sharding(writable)

      // create the byte array - allocate a single array up front to contain everything
      // ignore tier, not used here
      val bytes = Array.ofDim[Byte](sharing.length + shard.length + 10 + id.length)

      var i = 0
      if (!sharing.isEmpty) {
        bytes(0) = sharing.head // sharing is only a single byte
        i += 1
      }
      if (!shard.isEmpty) {
        bytes(i) = shard.head // shard is only a single byte
        i += 1
      }

      ByteArrays.writeShort(b, bytes, i)
      ByteArrays.writeLong(z, bytes, i + 2)
      System.arraycopy(id, 0, bytes, i + 10, id.length)

      SingleRowKeyValue(bytes, sharing, shard, Z3IndexKey(b, z), tier, id, writable.values)
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[Z3IndexKey]], tier: Boolean): Iterator[ByteRange] = {
      if (rangePrefixes.isEmpty) {
        ranges.map {
          case BoundedRange(lo, hi) =>
            BoundedByteRange(ByteArrays.toBytes(lo.bin, lo.z), ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z))

          case LowerBoundedRange(lo) =>
            BoundedByteRange(ByteArrays.toBytes(lo.bin, lo.z), ByteRange.UnboundedUpperRange)

          case UpperBoundedRange(hi) =>
            BoundedByteRange(ByteRange.UnboundedLowerRange, ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z))

          case UnboundedRange(_) =>
            BoundedByteRange(ByteRange.UnboundedLowerRange, ByteRange.UnboundedUpperRange)

          case r =>
            throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      } else {
        ranges.flatMap {
          case BoundedRange(lo, hi) =>
            val lower = ByteArrays.toBytes(lo.bin, lo.z)
            val upper = ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z)
            rangePrefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

          case LowerBoundedRange(lo) =>
            val lower = ByteArrays.toBytes(lo.bin, lo.z)
            val upper = ByteRange.UnboundedUpperRange
            rangePrefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

          case UpperBoundedRange(hi) =>
            val lower = ByteRange.UnboundedLowerRange
            val upper = ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z)
            rangePrefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

          case UnboundedRange(_) =>
            Seq(BoundedByteRange(ByteRange.UnboundedLowerRange, ByteRange.UnboundedUpperRange))

          case r =>
            throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      }
    }
  }
}
