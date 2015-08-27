/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.fixedwidth

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

case class FixedWidthField(name: String, transform: Transformers.Expr, s: Int, w: Int) extends Field {
  private val e = s + w
  private val mutableArray = Array.ofDim[Any](1)
  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    mutableArray(0) = args(0).asInstanceOf[String].substring(s, e)
    transform.eval(mutableArray)
  }
}

class FixedWidthConverterFactory extends SimpleFeatureConverterFactory[String] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "fixed-width")

  def buildConverter(targetSFT: SimpleFeatureType, conf: Config): FixedWidthConverter = {
    val fields    = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    new FixedWidthConverter(targetSFT, idBuilder, fields)
  }

  override def buildFields(fields: Seq[Config]): IndexedSeq[Field] = {
    fields.map { f =>
      val name = f.getString("name")
      val transform = Transformers.parseTransform(f.getString("transform"))
      if(f.hasPath("start") && f.hasPath("width")) {
        val s = f.getInt("start")
        val w = f.getInt("width")
        FixedWidthField(name, transform, s, w)
      } else SimpleField(name, transform)
    }.toIndexedSeq

  }
}

class FixedWidthConverter(val targetSFT: SimpleFeatureType,
                          val idBuilder: Transformers.Expr,
                          val inputFields: IndexedSeq[Field])
  extends ToSimpleFeatureConverter[String] {

  override def fromInputType(i: String): Seq[Array[Any]] = Seq(Array(i))

}
