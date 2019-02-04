/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.InputStream

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.{Counter, EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractCompositeConverter.CompositeEvaluationContext
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

  override def createEvaluationContext(globalParams: Map[String, Any],
      caches: Map[String, EnrichmentCache],
      counter: Counter): EvaluationContext = {
    val contexts = converters.map(_.createEvaluationContext(globalParams, caches, counter))
    new CompositeEvaluationContext(contexts.toIndexedSeq)
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val cec = ec match {
      case c: CompositeEvaluationContext => c
      case _ => new CompositeEvaluationContext(IndexedSeq.fill(converters.length)(ec))
    }
    val args = Array.ofDim[Any](1)
    var i = 0

    def eval(element: T): CloseableIterator[SimpleFeature] = {
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

    new ErrorHandlingIterator(parse(is, ec), errorMode, ec.counter).flatMap(eval)
  }

  override def close(): Unit = converters.foreach(CloseWithLogging.apply)
}

object AbstractCompositeConverter {

  class CompositeEvaluationContext(contexts: IndexedSeq[EvaluationContext]) extends EvaluationContext {

    private var current: EvaluationContext = contexts.headOption.orNull

    def setCurrent(i: Int): Unit = current = contexts(i)

    override def get(i: Int): Any = current.get(i)
    override def set(i: Int, v: Any): Unit = current.set(i, v)
    override def indexOf(n: String): Int = current.indexOf(n)
    override def counter: Counter = current.counter
    override def getCache(k: String): EnrichmentCache = current.getCache(k)
    override def clear(): Unit = contexts.foreach(_.clear())
  }
}
