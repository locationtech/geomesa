/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.index.index.LegacyTableNaming
import org.locationtech.geomesa.index.index.z3.legacy.XZ3IndexV1.XZ3IndexKeySpaceV1
import org.locationtech.geomesa.index.index.z3.{XZ3Index, XZ3IndexKeySpace, XZ3IndexValues, Z3IndexKey}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

// supports table sharing
class XZ3IndexV1(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode)
    extends XZ3Index(ds, sft, 1, geom, dtg, mode) with LegacyTableNaming[XZ3IndexValues, Z3IndexKey] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override protected val tableNameKey: String = "table.xz3.v1"

  override val keySpace: XZ3IndexKeySpace =
    new XZ3IndexKeySpaceV1(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom, dtg)
}

object XZ3IndexV1 {

  class XZ3IndexKeySpaceV1(sft: SimpleFeatureType,
                           override val sharing: Array[Byte],
                           sharding: ShardStrategy,
                           geomField: String,
                           dtgField: String)
      extends XZ3IndexKeySpace(sft, sharding, geomField, dtgField) {

    private val rangePrefixes = {
      if (sharing.isEmpty && sharding.length == 0) {
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
      val geom = writable.getAttribute[Geometry](geomIndex)
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
      }
      val envelope = geom.getEnvelopeInternal
      val dtg = writable.getAttribute[Date](dtgIndex)
      val time = if (dtg == null) { 0L } else { dtg.getTime }
      val BinnedTime(b, t) = timeToIndex(time)
      val xz = try {
        sfc.index(envelope.getMinX, envelope.getMinY, t, envelope.getMaxX, envelope.getMaxY, t, lenient)
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid xz value from geometry/time: $geom,$dtg", e)
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
      ByteArrays.writeLong(xz, bytes, i + 2)
      System.arraycopy(id, 0, bytes, i + 10, id.length)

      SingleRowKeyValue(bytes, sharing, shard, Z3IndexKey(b, xz), tier, id, writable.values)
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[Z3IndexKey]], tier: Boolean): Iterator[ByteRange] = {
      if (rangePrefixes.isEmpty) {
        ranges.map {
          case BoundedRange(lo, hi) =>
            BoundedByteRange(ByteArrays.toBytes(lo.bin, lo.z), ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z))

          case r =>
            throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      } else {
        ranges.flatMap {
          case BoundedRange(lo, hi) =>
            val lower = ByteArrays.toBytes(lo.bin, lo.z)
            val upper = ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z)
            rangePrefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

          case r =>
            throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      }
    }
  }
}
