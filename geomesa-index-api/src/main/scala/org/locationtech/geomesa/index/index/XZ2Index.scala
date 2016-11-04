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
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.opengis.feature.simple.SimpleFeatureType

trait XZ2Index[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, R] extends GeoMesaFeatureIndex[DS, F, W, Q]
    with IndexAdapter[DS, F, W, Q, R] with SpatialFilterStrategy[DS, F, W, Q] with LazyLogging {

  import IndexAdapter.{DefaultNumSplits, DefaultSplitArrays}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "xz2"

  override def supports(sft: SimpleFeatureType): Boolean = sft.nonPoints

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sfc = XZ2SFC(sft.getXZPrecision)
    val sharing = sft.getTableSharingBytes
    (wf) => Seq(createInsert(getRowKey(sfc, sharing, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sfc = XZ2SFC(sft.getXZPrecision)
    val sharing = sft.getTableSharingBytes
    (wf) => Seq(createDelete(getRowKey(sfc, sharing, wf), wf))
  }

  private def getRowKey(sfc: XZ2SFC, sharing: Array[Byte], wrapper: F): Array[Byte] = {
    val split = DefaultSplitArrays(wrapper.idHash % DefaultNumSplits)
    val envelope = wrapper.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    val xz = sfc.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val id = wrapper.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(sharing, split, Longs.toByteArray(xz), id)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    val start = if (sft.isTableSharing) { 10 } else { 9 } // table sharing + shard + 8 byte long
    (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val splits = DefaultSplitArrays.drop(1) // drop the first so we don't get an empty tablet
    if (sft.isTableSharing) {
      val sharing = sft.getTableSharingBytes
      splits.map(s => Bytes.concat(sharing, s))
    } else {
      splits
    }
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

    val sharing = sft.getTableSharingBytes

    // compute our accumulo ranges based on the coarse bounds for our query
    val ranges = if (filter.primary.isEmpty) { Seq(rangePrefix(sharing)) } else {
      import com.google.common.primitives.Bytes.concat

      val xy = geometries.map(GeometryUtils.bounds)

      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      val sfc = XZ2SFC(sft.getXZPrecision)
      val zs = sfc.ranges(xy, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))

      val prefixes = DefaultSplitArrays.map(concat(sharing, _))

      prefixes.flatMap { prefix =>
        zs.map { case (lo, hi) =>
          range(concat(prefix, lo), IndexAdapter.rowFollowingRow(concat(prefix, hi)))
        }
      }
    }

    scanPlan(sft, ds, filter, hints, ranges, filter.filter)
  }
}
