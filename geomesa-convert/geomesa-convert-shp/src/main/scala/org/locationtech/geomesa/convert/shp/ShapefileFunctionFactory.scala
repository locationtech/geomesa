/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.shp

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.shp.ShapefileFunctionFactory.{ShapefileAttribute, ShapefileFeatureId}
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}

import scala.collection.mutable.ArrayBuffer

class ShapefileFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(shpAttribute, shpFid)

  private val shpAttribute = new ShapefileAttribute(null, null)
  private val shpFid = new ShapefileFeatureId(null)
}

object ShapefileFunctionFactory {

  val InputSchemaKey = "geomesa.shp.attributes"
  val InputValuesKey = "geomesa.shp.values"

  class ShapefileAttribute(attrs: ArrayBuffer[String], values: ArrayBuffer[AnyRef])
      extends TransformerFunction {

    override val names: Seq[String] = Seq("shp")

    override def getInstance(args: List[Expression]): ShapefileAttribute = new ShapefileAttribute(null, null)

    override def apply(args: Array[AnyRef]): AnyRef = {
      val i = attrs.indexOf(args(0).asInstanceOf[String]) + 1 // 0 is fid
      if (i == 0) {
        throw new IllegalArgumentException(s"Attribute '${args(0)}' does not exist in shapefile: " +
            attrs.mkString(", "))
      }
      values(i)
    }

    override def withContext(ec: EvaluationContext): TransformerFunction = {
      val attrs = ec.accessor(InputSchemaKey).apply().asInstanceOf[ArrayBuffer[String]]
      if (attrs == null) {
        throw new IllegalArgumentException("Input schema not found in evaluation context, " +
            "'shp' function is not available")
      }
      val values = ec.accessor(InputValuesKey).apply().asInstanceOf[ArrayBuffer[AnyRef]]
      if (values == null) {
        throw new IllegalArgumentException("Input values not found in evaluation context, " +
            "'shp' function is not available")
      }
      new ShapefileAttribute(attrs, values)
    }
  }

  class ShapefileFeatureId(values: ArrayBuffer[AnyRef]) extends TransformerFunction {

    override val names: Seq[String] = Seq("shpFeatureId")

    override def getInstance(args: List[Expression]): ShapefileFeatureId = new ShapefileFeatureId(null)

    override def apply(args: Array[AnyRef]): AnyRef = values(0)

    override def withContext(ec: EvaluationContext): TransformerFunction = {
      val values = ec.accessor(InputValuesKey).apply().asInstanceOf[ArrayBuffer[AnyRef]]
      if (values == null) {
        throw new IllegalArgumentException("Input values not found in evaluation context, " +
            "'shpFeatureId' function is not available")
      }
      new ShapefileFeatureId(values)
    }
  }
}
