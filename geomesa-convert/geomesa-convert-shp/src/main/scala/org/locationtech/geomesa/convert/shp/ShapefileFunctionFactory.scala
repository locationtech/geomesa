/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.shp

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.shp.ShapefileFunctionFactory.{ShapefileAttribute, ShapefileFeatureId}
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}

class ShapefileFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(shpAttribute, shpFid)

  private val shpAttribute = new ShapefileAttribute
  private val shpFid = new ShapefileFeatureId
}

object ShapefileFunctionFactory {

  val InputSchemaKey = "geomesa.shp.attributes"
  val InputValuesKey = "geomesa.shp.values"

  class ShapefileAttribute extends TransformerFunction {

    private var i = -1
    private var values: Array[Any] = _

    override val names: Seq[String] = Seq("shp")

    override def getInstance: ShapefileAttribute = new ShapefileAttribute()

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (i == -1) {
        val names = ctx.get(ctx.indexOf(InputSchemaKey)).asInstanceOf[Array[String]]
        if (names == null) {
          throw new IllegalArgumentException("Input schema not found in evaluation context, " +
              "'shp' function is not available")
        }
        i = names.indexOf(args(0).asInstanceOf[String]) + 1 // 0 is fid
        if (i == 0) {
          throw new IllegalArgumentException(s"Attribute '${args(0)}' does not exist in shapefile: " +
              names.mkString(", "))
        }
        values = ctx.get(ctx.indexOf(InputValuesKey)).asInstanceOf[Array[Any]]
        if (values == null) {
          throw new IllegalArgumentException("Input values not found in evaluation context, " +
              "'shp' function is not available")
        }
      }
      values(i)
    }
  }

  class ShapefileFeatureId extends TransformerFunction {

    private var values: Array[Any] = _

    override val names: Seq[String] = Seq("shpFeatureId")

    override def getInstance: ShapefileFeatureId = new ShapefileFeatureId()

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (values == null) {
        values = ctx.get(ctx.indexOf(InputValuesKey)).asInstanceOf[Array[Any]]
        if (values == null) {
          throw new IllegalArgumentException("Input values not found in evaluation context, " +
              "'shpFeatureId' function is not available")
        }
      }
      values(0)
    }
  }
}
