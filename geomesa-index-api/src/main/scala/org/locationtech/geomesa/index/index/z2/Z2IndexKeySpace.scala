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
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.conf.QueryHints.LOOSE_BBOX
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

object Z2IndexKeySpace extends Z2IndexKeySpace {
  override val sfc: Z2SFC = Z2SFC
}

trait Z2IndexKeySpace extends IndexKeySpace[Z2IndexValues, Long] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def sfc: Z2SFC

  override val indexKeyByteLength: Int = 8

  override def supports(sft: SimpleFeatureType): Boolean = sft.isPoints

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): SimpleFeature => Seq[Long] = {
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    getZValue(geomIndex, lenient)
  }

  override def toIndexKeyBytes(sft: SimpleFeatureType, lenient: Boolean): ToIndexKeyBytes = {
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    getZValueBytes(geomIndex, lenient)
  }

  override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): Z2IndexValues = {
    import org.locationtech.geomesa.filter.FilterHelper._

    // TODO GEOMESA-2377 clean up duplicate code blocks in Z2/XZ2/Z3/XZ3IndexKeySpace

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, sft.getGeomField, sft.isPoints)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    explain(s"Geometries: $geometries")

    if (geometries.disjoint) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
      return Z2IndexValues(sfc, geometries, Seq.empty)
    }

    // compute our ranges based on the coarse bounds for our query
    val xy: Seq[(Double, Double, Double, Double)] = {
      val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
      val bits = QueryProperties.PolygonDecompBits.toInt.get
      geometries.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
    }

    Z2IndexValues(sfc, geometries, xy)
  }

  override def getRanges(values: Z2IndexValues, multiplier: Int): Iterator[ScanRange[Long]] = {
    val Z2IndexValues(_, _, xy) = values
    if (xy.isEmpty) { Iterator.empty } else {
      // note: `target` will always be Some, as ScanRangesTarget has a default value
      val target = QueryProperties.ScanRangesTarget.option.map(t => math.max(1, t.toInt / multiplier))
      sfc.ranges(xy, 64, target).iterator.map(r => BoundedRange(r.lower, r.upper))
    }
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[Long]],
                             prefixes: Seq[Array[Byte]],
                             tier: Boolean): Iterator[ByteRange] = {
    if (prefixes.isEmpty) {
      ranges.map {
        case BoundedRange(lo, hi) => BoundedByteRange(ByteArrays.toBytes(lo), ByteArrays.toBytesFollowingPrefix(hi))
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case BoundedRange(lo, hi) =>
          val lower = ByteArrays.toBytes(lo)
          val upper = ByteArrays.toBytesFollowingPrefix(hi)
          prefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  override def useFullFilter(values: Option[Z2IndexValues],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = {
    // if the user has requested strict bounding boxes, we apply the full filter
    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(config.forall(_.looseBBox))
    lazy val simpleGeoms = values.toSeq.flatMap(_.geometries.values).forall(GeometryUtils.isRectangular)

    !looseBBox || !simpleGeoms
  }

  private def getZValue(geomIndex: Int, lenient: Boolean)(feature: SimpleFeature): Seq[Long] = {
    val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
    }
    try { Seq(sfc.index(geom.getX, geom.getY, lenient).z) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry: $geom", e)
    }
  }

  private def getZValueBytes(geomIndex: Int,
                             lenient: Boolean)
                            (prefix: Seq[Array[Byte]],
                             feature: SimpleFeature,
                             suffix: Array[Byte]): Seq[Array[Byte]] = {
    val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
    }

    val z = try { sfc.index(geom.getX, geom.getY, lenient).z } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry: $geom", e)
    }

    // create the byte array - allocate a single array up front to contain everything
    val bytes = Array.ofDim[Byte](prefix.map(_.length).sum + 8 + suffix.length)
    var i = 0
    prefix.foreach { p => System.arraycopy(p, 0, bytes, i, p.length); i += p.length }
    ByteArrays.writeLong(z, bytes, i)
    System.arraycopy(suffix, 0, bytes, i + 8, suffix.length)
    Seq(bytes)
  }
}
