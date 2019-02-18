/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2

import org.locationtech.jts.geom.{Geometry, Point}
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.{NoShardStrategy, ZShardStrategy}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class XZ2IndexKeySpace(val sft: SimpleFeatureType, val sharding: ShardStrategy, geomField: String)
    extends IndexKeySpace[XZ2IndexValues, Long] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  require(classOf[Geometry].isAssignableFrom(sft.getDescriptor(geomField).getType.getBinding),
    s"Expected field $geomField to have a geometry binding, but instead it has: " +
        sft.getDescriptor(geomField).getType.getBinding.getSimpleName)

  protected val geomIndex: Int = sft.indexOf(geomField)
  protected val sfc = XZ2SFC(sft.getXZPrecision)

  private val isPoints = sft.getDescriptor(geomIndex).getType.getBinding == classOf[Point]

  override val attributes: Seq[String] = Seq(geomField)

  override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(8 + sharding.length)

  override val sharing: Array[Byte] = Array.empty

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
    val bytes = Array.ofDim[Byte](shard.length + 8 + id.length)

    if (shard.isEmpty) {
      ByteArrays.writeLong(xz, bytes, 0)
      System.arraycopy(id, 0, bytes, 8, id.length)
    } else {
      bytes(0) = shard.head // shard is only a single byte
      ByteArrays.writeLong(xz, bytes, 1)
      System.arraycopy(id, 0, bytes, 9, id.length)
    }

    SingleRowKeyValue(bytes, sharing, shard, xz, tier, id, writable.values)
  }

  override def getIndexValues(filter: Filter, explain: Explainer): XZ2IndexValues = {

    val geometries: FilterValues[Geometry] = {
      val extracted = FilterHelper.extractGeometries(filter, geomField, isPoints)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    explain(s"Geometries: $geometries")

    // compute our ranges based on the coarse bounds for our query
    val xy: Seq[(Double, Double, Double, Double)] = {
      val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
      val bits = QueryProperties.PolygonDecompBits.toInt.get
      geometries.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
    }

    XZ2IndexValues(sfc, geometries, xy)
  }

  override def getRanges(values: XZ2IndexValues, multiplier: Int): Iterator[ScanRange[Long]] = {
    val XZ2IndexValues(sfc, _, xy) = values
    // note: `target` will always be Some, as ScanRangesTarget has a default value
    val target = QueryProperties.ScanRangesTarget.option.map(t => math.max(1, t.toInt / multiplier))
    sfc.ranges(xy, target).iterator.map(r => BoundedRange(r.lower, r.upper))
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[Long]], tier: Boolean): Iterator[ByteRange] = {
    if (sharding.length == 0) {
      ranges.map {
        case BoundedRange(lo, hi) => BoundedByteRange(ByteArrays.toBytes(lo), ByteArrays.toBytesFollowingPrefix(hi))
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case BoundedRange(lo, hi) =>
          val lower = ByteArrays.toBytes(lo)
          val upper = ByteArrays.toBytesFollowingPrefix(hi)
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  // always apply the full filter to xz queries
  override def useFullFilter(values: Option[XZ2IndexValues],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = true
}

object XZ2IndexKeySpace extends IndexKeySpaceFactory[XZ2IndexValues, Long] {

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    attributes.lengthCompare(1) == 0 && sft.indexOf(attributes.head) != -1 &&
        classOf[Geometry].isAssignableFrom(sft.getDescriptor(attributes.head).getType.getBinding)

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): XZ2IndexKeySpace = {
    val shards = if (tier) { NoShardStrategy } else { ZShardStrategy(sft) }
    new XZ2IndexKeySpace(sft, shards, attributes.head)
  }
}
