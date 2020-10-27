/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.LegacyZ2SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait Z2Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R] extends GeoMesaFeatureIndex[DS, F, W]
    with IndexAdapter[DS, F, W, R, Z2IndexValues] with SpatialFilterStrategy[DS, F, W] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "z2"
  override val version: Int = 1

  override def supports(sft: SimpleFeatureType): Boolean = Z2Index.supports(sft)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = Z2Index.toIndexKey(sft)
    (wf) => Seq(createInsert(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = Z2Index.toIndexKey(sft, lenient = true)
    (wf) => Seq(createDelete(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  private def getRowKey(sharing: Array[Byte],
                        shards: IndexedSeq[Array[Byte]],
                        toIndexKey: (SimpleFeature) => Array[Byte],
                        wrapper: F): Array[Byte] = {
    val split = shards(wrapper.idHash % shards.length)
    val z = toIndexKey(wrapper.feature)
    Bytes.concat(sharing, split, z, wrapper.idBytes)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    val start = if (sft.isTableSharing) { 10 } else { 9 } // table sharing + shard + 8 byte long
    (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val splits = SplitArrays(sft).drop(1) // drop the first so we don't get an empty tablet
    if (sft.isTableSharing) {
      val sharing = sft.getTableSharingBytes
      splits.map(s => Bytes.concat(sharing, s))
    } else {
      splits
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W],
                            hints: Hints,
                            explain: Explainer,
                            interceptors: Seq[QueryInterceptor] = Seq()): QueryPlan[DS, F, W] = {

    val sharing = sft.getTableSharingBytes

    val (ranges, indexValues) = filter.primary match {
      case None =>
        filter.secondary.foreach { f =>
          logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
        }
        (Seq(rangePrefix(sharing)), None)

      case Some(f) =>
        val splits = SplitArrays(sft)
        val prefixes = if (sharing.length == 0) { splits } else { splits.map(Bytes.concat(sharing, _)) }
        val indexValues = Z2Index.getIndexValues(sft, f, explain)
        val ranges = Z2Index.getRanges(sft, indexValues).flatMap { case (s, e) =>
          prefixes.map(p => range(Bytes.concat(p, s), Bytes.concat(p, e)))
        }.toSeq
        (ranges, Some(indexValues))
    }

    val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(ds.config.looseBBox)

    // if the user has requested strict bounding boxes, we apply the full filter
    // if this is a non-point geometry type, the index is coarse-grained, so we apply the full filter
    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    lazy val simpleGeoms = indexValues.toSeq.flatMap(_.geometries.values).forall(GeometryUtils.isRectangular)

    val ecql = if (looseBBox && simpleGeoms) { filter.secondary } else { filter.filter }

    interceptors.foreach { _.guard(filter, indexValues).foreach{ throw _ } }

    scanPlan(sft, ds, filter, indexValues, ranges, ecql, hints)
  }
}

object Z2Index extends IndexKeySpace[Z2IndexValues] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val indexKeyLength: Int = 8

  override def supports(sft: SimpleFeatureType): Boolean = sft.isPoints

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): (SimpleFeature) => Array[Byte] = {
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    (feature) => {
      val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
      }
      val z = try { LegacyZ2SFC.index(geom.getX, geom.getY, lenient).z } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry: $geom", e)
      }
      Longs.toByteArray(z)
    }
  }

  override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): Z2IndexValues = {

    import org.locationtech.geomesa.filter.FilterHelper._

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, sft.getGeomField, sft.isPoints)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    explain(s"Geometries: $geometries")

    if (geometries.disjoint) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
      return Z2IndexValues(geometries, Seq.empty)
    }

    val xy = geometries.values.map(GeometryUtils.bounds)
    Z2IndexValues(geometries, xy)
  }

  override def getRanges(sft: SimpleFeatureType, values: Z2IndexValues): Iterator[(Array[Byte], Array[Byte])] = {
    val Z2IndexValues(geometries, xy) = values

    if (geometries.disjoint) {
      Iterator.empty
    } else {
      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

      val zs = LegacyZ2SFC.ranges(xy, 64, rangeTarget)
      zs.iterator.map(r => (Longs.toByteArray(r.lower), ByteArrays.toBytesFollowingPrefix(r.upper)))
    }
  }
}

case class Z2IndexValues(geometries: FilterValues[Geometry], bounds: Seq[(Double, Double, Double, Double)]) extends SpatialIndexValues {
  override def spatialBounds: Seq[(Double, Double, Double, Double)] = bounds
}
