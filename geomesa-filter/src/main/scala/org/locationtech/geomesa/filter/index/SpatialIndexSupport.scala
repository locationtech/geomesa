/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.index

import org.geotools.data.simple.{DelegateSimpleFeatureReader, FilteringSimpleFeatureReader, SimpleFeatureReader}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
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

  def getReaderForFilter(filter: Filter): SimpleFeatureReader = filter match {
    case f: IncludeFilter => include()
    case f: BinarySpatialOperator => spatial(f)
    case f: And => and(f)
    case f: Or => or(f)
    case f => unoptimized(f)
  }

  def include(): SimpleFeatureReader = reader(allFeatures())

  def spatial(f: BinarySpatialOperator): SimpleFeatureReader = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val geometries = FilterHelper.extractGeometries(f, sft.getGeomField)
    if (geometries.isEmpty) {
      unoptimized(f)
    } else {
      val envelope = geometries.head.getEnvelopeInternal
      geometries.tail.foreach(g => envelope.expandToInclude(g.getEnvelopeInternal))
      reader(spatialIndex.query(envelope, f.evaluate))
    }
  }

  def and(a: And): SimpleFeatureReader = {
    val geometries = extractGeometries(a, sft.getGeometryDescriptor.getLocalName)
    if (geometries.isEmpty) {
      unoptimized(a)
    } else {
      val envelope = geometries.head.getEnvelopeInternal
      geometries.tail.foreach(g => envelope.expandToInclude(g.getEnvelopeInternal))
      reader(spatialIndex.query(envelope, a.evaluate))
    }
  }

  def or(o: Or): SimpleFeatureReader = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureReader

    val readers = o.getChildren.map(getReaderForFilter).map(_.toIterator)
    val composed = readers.foldLeft(Iterator[SimpleFeature]())(_ ++ _)
    reader(composed)
  }

  def unoptimized(f: Filter): SimpleFeatureReader = new FilteringSimpleFeatureReader(include(), f)

  protected def reader(iter: Iterator[SimpleFeature]): SimpleFeatureReader =
    new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(iter))
}
