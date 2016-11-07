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
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.opengis.feature.simple.SimpleFeatureType

trait Z2Index[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, R] extends GeoMesaFeatureIndex[DS, F, W, Q]
    with IndexAdapter[DS, F, W, Q, R] with SpatialFilterStrategy[DS, F, W, Q] with LazyLogging {

  import IndexAdapter.{DefaultNumSplits, DefaultSplitArrays}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "z2"
  override val version: Int = 1

  override def supports(sft: SimpleFeatureType): Boolean = sft.isPoints

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => W = {
    val sharing = sft.getTableSharingBytes
    (wf) => createInsert(getRowKey(sharing, wf), wf)
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => W = {
    val sharing = sft.getTableSharingBytes
    (wf) => createDelete(getRowKey(sharing, wf), wf)
  }

  private def getRowKey(sharing: Array[Byte], wrapper: F): Array[Byte] = {
    val split = DefaultSplitArrays(wrapper.idHash % DefaultNumSplits)
    val geom = wrapper.feature.getDefaultGeometry.asInstanceOf[Point]
    val z = Z2SFC.index(geom.getX, geom.getY).z
    val id = wrapper.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(sharing, split, Longs.toByteArray(z), id)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val start = if (sft.isTableSharing) { 10 } else { 9 }
    (row: Array[Byte]) => new String(row, start, row.length - start, StandardCharsets.UTF_8)
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W, Q],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W, Q] = {
    import org.locationtech.geomesa.filter.FilterHelper._

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    explain(s"Geometries: $geometries")

    if (geometries == DisjointGeometries) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
      return scanPlan(sft, ds, filter, hints, Seq.empty, None)
    }

    val sharing = sft.getTableSharingBytes

    // compute our accumulo ranges based on the coarse bounds for our query
    val ranges = if (filter.primary.isEmpty) { Seq(rangePrefix(sharing)) } else {
      val xy = geometries.map(GeometryUtils.bounds)

      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      val zs = Z2SFC.ranges(xy, 64, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      val prefixes = DefaultSplitArrays.map(Bytes.concat(sharing, _))

      prefixes.flatMap { prefix =>
        zs.map { case (lo, hi) => range(Bytes.concat(prefix, lo), Bytes.concat(prefix, hi)) }
      }
    }

    scanPlan(sft, ds, filter, hints, ranges, filter.filter)
  }
}
