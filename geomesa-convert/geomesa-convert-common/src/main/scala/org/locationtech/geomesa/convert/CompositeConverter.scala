/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{DefaultCounter, Counter, EvaluationContext, Predicate}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

class CompositeConverterFactory[I] extends SimpleFeatureConverterFactory[I] {
  override def canProcess(conf: Config): Boolean = canProcessType(conf, "composite-converter")

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I] = {
    val converters: Seq[(Predicate, SimpleFeatureConverter[I])] =
      conf.getConfigList("converters").map { c =>
        val pred = Transformers.parsePred(c.getString("predicate"))
        val converter = SimpleFeatureConverters.build[I](sft, conf.getConfig(c.getString("converter")))
        (pred, converter)
      }
    new CompositeConverter[I](sft, converters)
  }

}

class CompositeConverter[I](val targetSFT: SimpleFeatureType,
                            converters: Seq[(Predicate, SimpleFeatureConverter[I])])
  extends SimpleFeatureConverter[I] {

  val evaluationContexts = List.fill(converters.length)(new EvaluationContext(null, null))

  def processWithCallback(gParams: Map[String, Any] = Map.empty, counter: Counter = new DefaultCounter): (I) => Seq[SimpleFeature] = {
    var count = 0
    (input: I) => {
      count += 1
      converters.view.zipWithIndex.flatMap { case ((pred, conv), i) =>
        implicit val ec = evaluationContexts(i)
        ec.getCounter.setLineCount(count)
        processIfValid(input, pred, conv, gParams)
      }.headOption
    }.toSeq
  }

  override def processInput(is: Iterator[I],  gParams: Map[String, Any] = Map.empty, counter: Counter = new DefaultCounter): Iterator[SimpleFeature] =
    is.flatMap(processWithCallback(gParams, counter))

  override def processSingleInput(i: I, gParams: Map[String, Any] = Map.empty)(implicit ec: EvaluationContext): Seq[SimpleFeature] =
    throw new UnsupportedOperationException("Single input processing is not enabled with composite converters...yet")

  private val mutableArray = Array.ofDim[Any](1)

  def processIfValid(input: I,
                     pred: Predicate,
                     conv: SimpleFeatureConverter[I],
                     gParams: Map[String, Any])
                    (implicit  ec: EvaluationContext) = {
    val opt =
      Try {
        mutableArray(0) = input
        pred.eval(mutableArray)
      }.toOption.toSeq

    opt.flatMap { v => if (v) conv.processSingleInput(input, gParams)(ec) else Seq.empty }
  }
}
