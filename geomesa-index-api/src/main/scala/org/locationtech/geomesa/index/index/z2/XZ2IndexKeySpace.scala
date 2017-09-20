/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2

import com.google.common.primitives.Longs
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

object XZ2IndexKeySpace extends XZ2IndexKeySpace

trait XZ2IndexKeySpace extends IndexKeySpace[XZ2ProcessingValues] {

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
      val xz = try { sfc.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY, lenient) } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid xz value from geometry: $geom", e)
      }
      Longs.toByteArray(xz)
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

    // compute our ranges based on the coarse bounds for our query

    val sfc = XZ2SFC(sft.getXZPrecision)
    val xy = geometries.values.map(GeometryUtils.bounds)

    // make our underlying index values available to other classes in the pipeline for processing
    processingValues.set(XZ2ProcessingValues(sfc, geometries, xy))

    val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

    val zs = sfc.ranges(xy, rangeTarget)
    zs.iterator.map(r => (Longs.toByteArray(r.lower), ByteArrays.toBytesFollowingPrefix(r.upper)))
  }
}
