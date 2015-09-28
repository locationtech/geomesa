/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.filter.visitor

import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.spatial._

trait SafeTopologicalFilterVisitor extends DuplicatingFilterVisitor {

  def sft: SimpleFeatureType

  override def visit(dw: DWithin, data: AnyRef) = FilterHelper.rewriteDwithin(dw)
  override def visit(op: BBOX, data: AnyRef) = FilterHelper.visitBBOX(op, sft)
  override def visit(op: Within, data: AnyRef) = FilterHelper.visitBinarySpatialOp(op, sft)
  override def visit(op: Intersects, data: AnyRef) = FilterHelper.visitBinarySpatialOp(op, sft)
  override def visit(op: Overlaps, data: AnyRef) = FilterHelper.visitBinarySpatialOp(op, sft)
}

class SafeTopologicalFilterVisitorImpl(val sft: SimpleFeatureType) extends SafeTopologicalFilterVisitor
