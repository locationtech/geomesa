/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import com.codahale.metrics.Counter
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert.EvaluationContext.{EvaluationError, FieldAccessor}
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.{EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractCompositeConverter.{CompositeEvaluationContext, PredicateContext}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.InputStream
import scala.util.control.NonFatal

abstract class AbstractCompositeConverter[T <: AnyRef](
    sft: SimpleFeatureType,
    errorMode: ErrorMode,
    delegates: Seq[(Predicate, ParsingConverter[T])]
  ) extends SimpleFeatureConverter with LazyLogging {

  protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[T]

  override def targetSft: SimpleFeatureType = sft

  override def createEvaluationContext(globalParams: Map[String, Any]): EvaluationContext =
    CompositeEvaluationContext(delegates.map(_._2), globalParams)

  override def createEvaluationContext(
      globalParams: Map[String, Any],
      success: Counter,
      failure: Counter): EvaluationContext = {
    CompositeEvaluationContext(delegates.map(_._2), globalParams, success, failure)
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val contexts = ec match {
      case cec: CompositeEvaluationContext => cec.contexts.iterator
      case _ => Iterator.continually(ec)
    }
    val predicates = delegates.map { case (predicate, converter) =>
      if (!contexts.hasNext) {
        throw new IllegalArgumentException(s"Invalid evaluation context doesn't match this converter: $ec")
      }
      val context = contexts.next()
      PredicateContext(predicate.withContext(context), converter, context)
    }

    val args = Array.ofDim[AnyRef](1)

    def matches(p: Predicate): Boolean = {
      try { p.apply(args) } catch {
        case NonFatal(e) => logger.warn(s"Error evaluating predicate $p: ", e); false
      }
    }

    val routing = predicates.head.context.metrics.histogram("predicate.eval.nanos")

    def eval(element: T): CloseableIterator[SimpleFeature] = {
      val start = System.nanoTime()
      args(0) = element
      val converted = predicates.collectFirst { case p if matches(p.predicate) =>
        routing.update(System.nanoTime() - start) // note: invoke before executing the conversion
        p.context.line = ec.line // update the line in the sub context
        p.converter.convert(CloseableIterator.single(element), p.context)
      }
      converted.getOrElse {
        routing.update(System.nanoTime() - start)
        ec.failure.inc()
        CloseableIterator.empty
      }
    }

    val hist = predicates.head.context.metrics.histogram("parse.nanos")
    new ErrorHandlingIterator(parse(is, ec), errorMode, ec.failure, hist).flatMap(eval)
  }

  override def close(): Unit = CloseWithLogging(delegates.map(_._2))
}

object AbstractCompositeConverter {

<<<<<<< HEAD
  case class CompositeEvaluationContext(contexts: Seq[EvaluationContext], success: Counter, failure: Counter)
      extends EvaluationContext {
=======
  // noinspection ScalaDeprecation
  case class CompositeEvaluationContext(contexts: Seq[EvaluationContext], success: Counter, failure: Counter)
      extends EvaluationContext {
    override def get(i: Int): Any = throw new NotImplementedError()
    override def set(i: Int, v: Any): Unit = throw new NotImplementedError()
    override def indexOf(n: String): Int = throw new NotImplementedError()
    override def clear(): Unit = throw new NotImplementedError()
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
    override def cache: Map[String, EnrichmentCache] = throw new NotImplementedError()
    override def metrics: ConverterMetrics = throw new NotImplementedError()
    override def accessor(name: String): FieldAccessor = throw new NotImplementedError()
    override def evaluate(args: Array[AnyRef]): Either[EvaluationError, Array[AnyRef]] =
      throw new NotImplementedError()
  }

  object CompositeEvaluationContext {

    def apply(
        converters: Seq[_ <: SimpleFeatureConverter],
        globalParams: Map[String, Any]): CompositeEvaluationContext = {
      // create a single set of shared success/failure counters
      val head = converters.head.createEvaluationContext(globalParams)
      val tail = converters.tail.map(_.createEvaluationContext(globalParams, head.success, head.failure))
      CompositeEvaluationContext(head +: tail, head.success, head.failure)
    }

    def apply(
        converters: Seq[_ <: SimpleFeatureConverter],
        globalParams: Map[String, Any],
        success: Counter,
        failure: Counter): CompositeEvaluationContext = {
      val contexts = converters.map(_.createEvaluationContext(globalParams, success, failure))
      CompositeEvaluationContext(contexts, success, failure)
    }
  }

  case class PredicateContext[T <: SimpleFeatureConverter](
      predicate: Predicate,
      converter: T,
      context: EvaluationContext
    )
}
