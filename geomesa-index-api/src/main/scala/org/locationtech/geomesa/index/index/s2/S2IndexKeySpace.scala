/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.s2

import org.geotools.util.factory.Hints
import org.locationtech.geomesa.curve.S2SFC
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.index.api
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.{NoShardStrategy, ZShardStrategy}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryHints.LOOSE_BBOX
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.jts.geom.{Geometry, Point}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
  * @author sunyabo 2019年07月24日 14:42
  * @version V1.0
  */
class S2IndexKeySpace(val sft: SimpleFeatureType, val sharding: ShardStrategy, geomField: String)
  extends IndexKeySpace[S2IndexValues, Long] {

  require(classOf[Point].isAssignableFrom(sft.getDescriptor(geomField).getType.getBinding),
    s"Expected field $geomField to have a point binding, but instead it has: " +
      sft.getDescriptor(geomField).getType.getBinding.getSimpleName)

  private val sfc: S2SFC =
    S2SFC(
      QueryProperties.S2MinLevel,
      QueryProperties.S2MaxLevel,
      QueryProperties.S2LevelMod,
      QueryProperties.S2MaxCells
    )

  private val geomIndex: Int = sft.indexOf(geomField)

  /**
    * The attributes used to create the index keys
    *
    * @return
    */
  override val attributes: Seq[String] = Seq(geomField)

  /**
    * Length of an index key. If static (general case), will return a Right with the length. If dynamic,
    * will return Left with a function to determine the length from a given (row, offset, length)
    *
    * @return
    */
  override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(8 + sharding.length)

  /**
    * Table sharing
    *
    * @return
    */
  override val sharing: Array[Byte] = Array.empty

  /**
    * Index key from the attributes of a simple feature
    *
    * @param writable simple feature with cached values
    * @param tier    tier bytes
    * @param id      feature id bytes
    * @param lenient if input values should be strictly checked, default false
    * @return
    */
  override def toIndexKey(writable: WritableFeature,
                          tier: Array[Byte],
                          id: Array[Byte],
                          lenient: Boolean): RowKeyValue[Long] = {
    val geom = writable.getAttribute[Point](geomIndex)
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
    }
    val s = try { sfc.index(geom.getX, geom.getY, lenient) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid s value from geometry: $geom", e)
    }
    val shard = sharding(writable)

    // create the byte array - allocate a single array up front to contain everything
    // ignore tier, not used here
    val bytes = Array.ofDim[Byte](shard.length + 8 + id.length)

    if (shard.isEmpty) {
      ByteArrays.writeLong(s, bytes)
      System.arraycopy(id, 0, bytes, 8, id.length)
    } else {
      bytes(0) = shard.head // shard is only a single byte
      ByteArrays.writeLong(s, bytes, 1)
      System.arraycopy(id, 0, bytes, 9, id.length)
    }

    SingleRowKeyValue(bytes, sharing, shard, s, tier, id, writable.values)
  }

  /**
    * Extracts values out of the filter used for range and push-down predicate creation
    *
    * @param filter  query filter
    * @param explain explainer
    * @return
    */
  override def getIndexValues(filter: Filter, explain: Explainer): S2IndexValues = {

    val geometries: FilterValues[Geometry] = {
      val extracted = FilterHelper.extractGeometries(filter, geomField) // intersect since we have points
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    explain(s"Geometries: $geometries")

    if (geometries.disjoint) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
      return S2IndexValues(sfc, geometries, Seq.empty)
    }

    // compute our ranges based on the coarse bounds for our query
    val xy: Seq[(Double, Double, Double, Double)] = {
      val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
      val bits = QueryProperties.PolygonDecompBits.toInt.get
      geometries.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
    }

    S2IndexValues(sfc, geometries, xy)
  }

  /**
    * Creates ranges over the index keys
    *
    * @param values     index values @see getIndexValues
    * @param multiplier hint for how many times the ranges will be multiplied. can be used to
    *                   inform the number of ranges generated
    * @return
    */
  override def getRanges(values: S2IndexValues, multiplier: Int): Iterator[ScanRange[Long]] = {
    val S2IndexValues(_, _, xy) = values
    if (xy.isEmpty) {
      Iterator.empty
    } else {
      // note: `target` will always be Some, as ScanRangesTarget has a default value
      val target = QueryProperties.ScanRangesTarget.option.map(t => math.max(1, t.toInt / multiplier))
      sfc.ranges(xy, -1, target).iterator.map(r => BoundedRange(r.lower, r.upper))
    }
  }

  /**
    * Creates bytes from ranges
    *
    * @param ranges typed scan ranges. @see `getRanges`
    * @param tier   will the ranges have tiered ranges appended, or not
    * @return
    */
  override def getRangeBytes(ranges: Iterator[ScanRange[Long]], tier: Boolean): Iterator[api.ByteRange] = {
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

  /**
    * Determines if the ranges generated by `getRanges` are sufficient to fulfill the query,
    * or if additional filtering needs to be done
    *
    * @param config data store config
    * @param values index values @see getIndexValues
    * @param hints  query hints
    * @return
    */
  override def useFullFilter(values: Option[S2IndexValues],
                             config: Option[GeoMesaDataStoreFactory.GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = {
    // if the user has requested strict bounding boxes, we apply the full filter
    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(config.forall(_.queries.looseBBox))
    lazy val simpleGeoms = values.toSeq.flatMap(_.geometries.values).forall(GeometryUtils.isRectangular)

    !looseBBox || !simpleGeoms
  }

}

object S2IndexKeySpace extends IndexKeySpaceFactory[S2IndexValues, Long] {

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    attributes.lengthCompare(1) == 0 && sft.indexOf(attributes.head) != -1 &&
      classOf[Point].isAssignableFrom(sft.getDescriptor(attributes.head).getType.getBinding)

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): S2IndexKeySpace = {
    val shards = if (tier) { NoShardStrategy } else { ZShardStrategy(sft) }
    new S2IndexKeySpace(sft, shards, attributes.head)
  }
}
