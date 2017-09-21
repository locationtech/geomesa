/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.index

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.spatial.BinarySpatialOperator

trait SpatialIndexSupport {

  import scala.collection.JavaConversions._

  def sft: SimpleFeatureType

  def spatialIndex: SpatialIndex[SimpleFeature]

  def allFeatures(): Iterator[SimpleFeature]

  def query(filter: Filter): Iterator[SimpleFeature] = filter match {
    case f: IncludeFilter => include()
    case f: BinarySpatialOperator => spatial(f)
    case f: And => and(f)
    case f: Or => or(f)
    case f => unoptimized(f)
  }

  def include(): Iterator[SimpleFeature] = allFeatures()

  def spatial(f: BinarySpatialOperator): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val geometries = FilterHelper.extractGeometries(f, sft.getGeomField, intersect = false)
    if (geometries.isEmpty) {
      unoptimized(f)
    } else {
      val envelope = geometries.values.head.getEnvelopeInternal
      geometries.values.tail.foreach(g => envelope.expandToInclude(g.getEnvelopeInternal))
      spatialIndex.query(envelope, f.evaluate)
    }
  }

  def and(a: And): Iterator[SimpleFeature] = {
    val geometries = extractGeometries(a, sft.getGeometryDescriptor.getLocalName, intersect = false)
    if (geometries.isEmpty) {
      unoptimized(a)
    } else {
      val envelope = geometries.values.head.getEnvelopeInternal
      geometries.values.tail.foreach(g => envelope.expandToInclude(g.getEnvelopeInternal))
      spatialIndex.query(envelope, a.evaluate)
    }
  }

  def or(o: Or): Iterator[SimpleFeature] = o.getChildren.map(query).reduceLeft(_ ++ _)

  def unoptimized(f: Filter): Iterator[SimpleFeature] = include().filter(f.evaluate)
}
