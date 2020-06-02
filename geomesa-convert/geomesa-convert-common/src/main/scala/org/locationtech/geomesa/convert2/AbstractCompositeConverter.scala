/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.InputStream

import com.codahale.metrics.Counter
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.{EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractCompositeConverter.CompositeEvaluationContext
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

abstract class AbstractCompositeConverter[T](
    sft: SimpleFeatureType,
    errorMode: ErrorMode,
    delegates: Seq[(Predicate, ParsingConverter[T])]
  ) extends SimpleFeatureConverter with LazyLogging {

  private val predicates = delegates.map(_._1).toArray
  private val converters = delegates.map(_._2).toArray

  protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[T]

  override def targetSft: SimpleFeatureType = sft

  override def createEvaluationContext(globalParams: Map[String, Any]): EvaluationContext =
    new CompositeEvaluationContext(converters.map(_.createEvaluationContext(globalParams)))

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val cec = ec match {
      case c: CompositeEvaluationContext => c
      case _ => new CompositeEvaluationContext(IndexedSeq.fill(converters.length)(ec))
    }
    val args = Array.ofDim[Any](1)
    var i = 0

    val routing = ec.metrics.histogram("predicate.eval.nanos")

    def eval(element: T): CloseableIterator[SimpleFeature] = {
      val start = System.nanoTime()
      args(0) = element
      i = 0
      while (i < predicates.length) {
        cec.setCurrent(i)
        val found = try { predicates(i).eval(args)(ec) } catch {
          case NonFatal(e) => logger.warn(s"Error evaluating predicate $i: ", e); false
        }
        if (found) {
          routing.update(System.nanoTime() - start) // note: invoke before executing the conversion
          return converters(i).convert(CloseableIterator.single(element), ec)
        }
        i += 1
      }
      routing.update(System.nanoTime() - start)
      ec.failure.inc()
      CloseableIterator.empty
    }

    val hist = ec.metrics.histogram("parse.nanos")
    new ErrorHandlingIterator(parse(is, ec), errorMode, ec.failure, hist).flatMap(eval)
  }

  override def close(): Unit = CloseWithLogging(converters)
}

object AbstractCompositeConverter {

  class CompositeEvaluationContext(contexts: IndexedSeq[EvaluationContext]) extends EvaluationContext {

    private var current: EvaluationContext = contexts.headOption.orNull

    def setCurrent(i: Int): Unit = current = contexts(i)

    override def get(i: Int): Any = current.get(i)
    override def set(i: Int, v: Any): Unit = current.set(i, v)
    override def indexOf(n: String): Int = current.indexOf(n)
    override def clear(): Unit = contexts.foreach(_.clear())
    override def cache: Map[String, EnrichmentCache] = current.cache
    override def metrics: ConverterMetrics = current.metrics
    override def success: Counter = current.success
    override def failure: Counter = current.failure
  }
}
