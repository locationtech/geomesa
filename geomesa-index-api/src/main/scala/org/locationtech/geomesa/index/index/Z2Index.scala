/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Point
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.index.utils.{Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.opengis.feature.simple.SimpleFeatureType

trait Z2Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R] extends GeoMesaFeatureIndex[DS, F, W]
    with IndexAdapter[DS, F, W, R] with SpatialFilterStrategy[DS, F, W] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "z2"
  override val version: Int = 1

  override def supports(sft: SimpleFeatureType): Boolean = sft.isPoints

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    (wf) => Seq(createInsert(getRowKey(sharing, shards, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    (wf) => Seq(createDelete(getRowKey(sharing, shards, wf), wf))
  }

  private def getRowKey(sharing: Array[Byte], shards: IndexedSeq[Array[Byte]], wrapper: F): Array[Byte] = {
    val split = shards(wrapper.idHash % shards.length)
    val geom = wrapper.feature.getDefaultGeometry.asInstanceOf[Point]
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${wrapper.feature.getID}")
    }
    val z = Z2SFC.index(geom.getX, geom.getY).z
    val id = wrapper.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(sharing, split, Longs.toByteArray(z), id)
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
                            explain: Explainer): QueryPlan[DS, F, W] = {
    import org.locationtech.geomesa.filter.FilterHelper._

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(FilterValues(Seq(WholeWorldPolygon)))

    explain(s"Geometries: $geometries")

    if (geometries.disjoint) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
      return scanPlan(sft, ds, filter, hints, Seq.empty, None)
    }

    val sharing = sft.getTableSharingBytes

    // compute our accumulo ranges based on the coarse bounds for our query
    val ranges = if (filter.primary.isEmpty) { Seq(rangePrefix(sharing)) } else {
      val xy = geometries.values.map(GeometryUtils.bounds)

      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      val zs = Z2SFC.ranges(xy, 64, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      val prefixes = SplitArrays(sft).map(Bytes.concat(sharing, _))

      prefixes.flatMap { prefix =>
        zs.map { case (lo, hi) =>
          range(Bytes.concat(prefix, lo), IndexAdapter.rowFollowingRow(Bytes.concat(prefix, hi)))
        }
      }
    }

    val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(ds.config.looseBBox)
    val ecql = if (looseBBox) { filter.secondary } else { filter.filter }

    scanPlan(sft, ds, filter, hints, ranges, ecql)
  }
}
