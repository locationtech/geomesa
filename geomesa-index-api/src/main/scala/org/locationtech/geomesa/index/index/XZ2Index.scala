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
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait XZ2Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R] extends GeoMesaFeatureIndex[DS, F, W]
    with IndexAdapter[DS, F, W, R, XZ2IndexValues] with SpatialFilterStrategy[DS, F, W] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "xz2"

  override def supports(sft: SimpleFeatureType): Boolean = XZ2Index.supports(sft)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = XZ2Index.toIndexKey(sft)
    (wf) => Seq(createInsert(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = XZ2Index.toIndexKey(sft, lenient = true)
    (wf) => Seq(createDelete(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  private def getRowKey(sharing: Array[Byte],
                        shards: IndexedSeq[Array[Byte]],
                        toIndexKey: (SimpleFeature) => Array[Byte],
                        wrapper: F): Array[Byte] = {
    val split = shards(wrapper.idHash % shards.length)
    val xz = toIndexKey(wrapper.feature)
    Bytes.concat(sharing, split, xz, wrapper.idBytes)
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
        val indexValues = XZ2Index.getIndexValues(sft, f, explain)
        val ranges = XZ2Index.getRanges(sft, indexValues).flatMap { case (s, e) =>
          prefixes.map(p => range(Bytes.concat(p, s), Bytes.concat(p, e)))
        }.toSeq
        (ranges, Some(indexValues))
    }

    interceptors.foreach { _.guard(filter, indexValues).foreach{ throw _ } }

    scanPlan(sft, ds, filter, indexValues, ranges, filter.filter, hints)
  }
}

object XZ2Index extends IndexKeySpace[XZ2IndexValues] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val indexKeyLength: Int = 8

  override def supports(sft: SimpleFeatureType): Boolean = sft.nonPoints

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): (SimpleFeature) => Array[Byte] = {
    val sfc = XZ2SFC(sft.getXZPrecision)
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)

    (feature) => {
      val geom = feature.getAttribute(geomIndex).asInstanceOf[Geometry]
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
      }
      val envelope = geom.getEnvelopeInternal
      val xz = try {
        sfc.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY, lenient)
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid xz value from geometry: $geom", e)
      }
      Longs.toByteArray(xz)
    }
  }

  override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): XZ2IndexValues = {
    import org.locationtech.geomesa.filter.FilterHelper._

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, sft.getGeomField, sft.isPoints)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    explain(s"Geometries: $geometries")

    // compute our ranges based on the coarse bounds for our query

    val sfc = XZ2SFC(sft.getXZPrecision)
    val xy = geometries.values.map(GeometryUtils.bounds)

    XZ2IndexValues(sfc, geometries, xy)
  }

  override def getRanges(sft: SimpleFeatureType, values: XZ2IndexValues): Iterator[(Array[Byte], Array[Byte])] = {
    val XZ2IndexValues(sfc, geometries, xy) = values

    val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

    val zs = sfc.ranges(xy, rangeTarget)
    zs.iterator.map(r => (Longs.toByteArray(r.lower), ByteArrays.toBytesFollowingPrefix(r.upper)))
  }
}

case class XZ2IndexValues(sfc: XZ2SFC, geometries: FilterValues[Geometry], bounds: Seq[(Double, Double, Double, Double)]) extends SpatialIndexValues {
  override def spatialBounds: Seq[(Double, Double, Double, Double)] = bounds
}

