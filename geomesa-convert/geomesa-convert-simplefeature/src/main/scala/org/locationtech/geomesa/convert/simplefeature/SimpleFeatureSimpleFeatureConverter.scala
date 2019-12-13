/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.simplefeature

import java.io.InputStream

import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

@deprecated("replaced with FeatureToFeatureConverter")
class SimpleFeatureSimpleFeatureConverter(inputSFT: SimpleFeatureType,
                                          val targetSFT: SimpleFeatureType,
                                          val idBuilder: Transformers.Expr,
                                          val inputFields: IndexedSeq[Field],
                                          val userDataBuilder: Map[String, Transformers.Expr],
                                          val caches: Map[String, EnrichmentCache],
                                          val parseOpts: ConvertParseOpts) extends ToSimpleFeatureConverter[SimpleFeature] {


  /**
    * Process a single input (e.g. line)
    */
  override def processSingleInput(i: SimpleFeature, ec: EvaluationContext): Iterator[SimpleFeature] = {
    import scala.collection.JavaConversions._
    ec.clear()
    ec.counter.incLineCount()

    val in = i.getAttributes ++ Array(i.getID)
    val res = convert(in.toArray, ec)
    if(res == null) ec.counter.incFailure()
    else ec.counter.incSuccess()
    Iterator.single(res)
  }


  override def fromInputType(i: SimpleFeature, ec: EvaluationContext): Iterator[Array[Any]] = ???

  override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] = ???
}
