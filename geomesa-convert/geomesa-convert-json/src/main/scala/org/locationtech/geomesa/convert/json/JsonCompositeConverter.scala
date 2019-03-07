/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.io.InputStream
import java.nio.charset.Charset

import com.google.gson.JsonElement
import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.convert.{Counter, EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.{ParsingConverter, SimpleFeatureConverter}
import org.locationtech.geomesa.convert2.composite.CompositeConverter.CompositeEvaluationContext
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

class JsonCompositeConverter(sft: SimpleFeatureType,
                             encoding: Charset,
                             delegates: Seq[(Predicate, ParsingConverter[JsonElement])])
    extends SimpleFeatureConverter with StrictLogging {

  private val predicates = delegates.map(_._1).toArray
  private val converters = delegates.map(_._2).toArray

  override def targetSft: SimpleFeatureType = sft

  override def createEvaluationContext(globalParams: Map[String, Any],
                                       caches: Map[String, EnrichmentCache],
                                       counter: Counter): EvaluationContext = {
    val contexts = converters.map(_.createEvaluationContext(globalParams, caches, counter))
    new CompositeEvaluationContext(contexts.toIndexedSeq)
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val elements = new JsonConverter.JsonIterator(is, encoding, ec.counter)
    val cec = ec match {
      case c: CompositeEvaluationContext => c
      case _ => new CompositeEvaluationContext(IndexedSeq.fill(converters.length)(ec))
    }
    val args = Array.ofDim[Any](1)
    var i = 0

    def eval(element: JsonElement): CloseableIterator[SimpleFeature] = {
      args(0) = element
      i = 0
      while (i < predicates.length) {
        cec.setCurrent(i)
        val found = try { predicates(i).eval(args)(ec) } catch {
          case NonFatal(e) => logger.warn(s"Error evaluating predicate $i: ", e); false
        }
        if (found) {
          return converters(i).convert(CloseableIterator.single(element), ec)
        }
        i += 1
      }
      ec.counter.incFailure()
      CloseableIterator.empty
    }

    elements.flatMap(eval)
  }

  override def close(): Unit = converters.foreach(CloseWithLogging.apply)
}
