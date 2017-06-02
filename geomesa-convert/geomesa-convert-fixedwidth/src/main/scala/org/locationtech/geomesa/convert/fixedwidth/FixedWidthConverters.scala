/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.fixedwidth

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.immutable.IndexedSeq

case class FixedWidthField(name: String, transform: Transformers.Expr, start: Int, width: Int) extends Field {
  private val endIdx: Int = start + width
  private val mutableArray = Array.ofDim[Any](1)
  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    mutableArray(0) = args(0).asInstanceOf[String].substring(start, endIdx)
    transform.eval(mutableArray)
  }
}

class FixedWidthConverterFactory extends AbstractSimpleFeatureConverterFactory[String] {

  override protected val typeToProcess = "fixed-width"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        parseOpts: ConvertParseOpts): SimpleFeatureConverter[String] = {
    new FixedWidthConverter(sft, idBuilder, fields, userDataBuilder, parseOpts)
  }

  override protected def buildField(field: Config): Field = {
    val name = field.getString("name")
    val transform = Transformers.parseTransform(field.getString("transform"))
    if (field.hasPath("start") && field.hasPath("width")) {
      val s = field.getInt("start")
      val w = field.getInt("width")
      FixedWidthField(name, transform, s, w)
    } else {
      SimpleField(name, transform)
    }
  }
}

class FixedWidthConverter(val targetSFT: SimpleFeatureType,
                          val idBuilder: Transformers.Expr,
                          val inputFields: IndexedSeq[Field],
                          val userDataBuilder: Map[String, Expr],
                          val parseOpts: ConvertParseOpts)
  extends LinesToSimpleFeatureConverter {

  override def fromInputType(i: String): Seq[Array[Any]] = Seq(Array(i))
}
