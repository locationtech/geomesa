/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2

import com.google.common.primitives.Longs
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

object Z2IndexKeySpace extends Z2IndexKeySpace {
  override val sfc: Z2SFC = Z2SFC
}

trait Z2IndexKeySpace extends IndexKeySpace[Z2ProcessingValues] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def sfc: Z2SFC

  override val indexKeyLength: Int = 8

  override def supports(sft: SimpleFeatureType): Boolean = sft.isPoints

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): (SimpleFeature) => Array[Byte] = {
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    (feature) => {
      val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
      }
      val z = try { sfc.index(geom.getX, geom.getY, lenient).z } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry: $geom", e)
      }
      Longs.toByteArray(z)
    }
  }

  override def getRanges(sft: SimpleFeatureType,
                         filter: Filter,
                         explain: Explainer): Iterator[(Array[Byte], Array[Byte])] = {

    import org.locationtech.geomesa.filter.FilterHelper._

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, sft.getGeomField, sft.isPoints)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    explain(s"Geometries: $geometries")

    if (geometries.disjoint) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
      return Iterator.empty
    }

    val xy = geometries.values.map(GeometryUtils.bounds)

    // make our underlying index values available to other classes in the pipeline for processing
    processingValues.set(Z2ProcessingValues(sfc, geometries, xy))

    val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

    val zs = sfc.ranges(xy, 64, rangeTarget)
    zs.iterator.map(r => (Longs.toByteArray(r.lower), ByteArrays.toBytesFollowingPrefix(r.upper)))
  }
}
