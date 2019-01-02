/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.rules

import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.jts.JTSTypes.GeometryTypeInstance
import org.apache.spark.sql.types.DataType

/**
 * Catalyst AST expression used during rule rewriting to extract geometry literal values
 * from Catalyst memory and keep a copy in JVM heap space for subsequent use in rule evaluation.
 */
case class GeometryLiteral(repr: InternalRow, geom: Geometry) extends LeafExpression  with CodegenFallback {

  override def foldable: Boolean = true

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = repr

  override def dataType: DataType = GeometryTypeInstance
}
