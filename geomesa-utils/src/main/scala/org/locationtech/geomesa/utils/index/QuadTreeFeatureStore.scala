/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Geometry
import org.geotools.geometry.jts.JTS
import org.locationtech.geomesa.utils.geotools.{DFI, DFR, FR}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}

import scala.collection.JavaConverters._

trait QuadTreeFeatureStore {

  def spatialIndex: SpatialIndex[SimpleFeature]
  def sft: SimpleFeatureType

  def within(w: Within): FR = {
    val (_, geomLit) = splitBinOp(w)
    val geom = geomLit.evaluate(null).asInstanceOf[Geometry]
    val res = spatialIndex.query(geom.getEnvelopeInternal, w.evaluate)
    val fiter = new DFI(res.asJava)
    new DFR(sft, fiter)
  }

  def bbox(b: BBOX): FR = {
    val bounds = JTS.toGeometry(b.getBounds)
    val res = spatialIndex.query(bounds.getEnvelopeInternal, b.evaluate)
    val fiter = new DFI(res.asJava)
    new DFR(sft, fiter)
  }

  def splitBinOp(binop: BinarySpatialOperator): (PropertyName, Literal) =
    binop.getExpression1 match {
      case pn: PropertyName => (pn, binop.getExpression2.asInstanceOf[Literal])
      case l: Literal       => (binop.getExpression2.asInstanceOf[PropertyName], l)
    }

}
