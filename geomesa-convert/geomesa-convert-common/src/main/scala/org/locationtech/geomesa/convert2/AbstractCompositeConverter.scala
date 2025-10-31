/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import com.codahale.metrics.Counter
import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.{Metrics, Timer}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert.EvaluationContext.{ContextListener, EvaluationError, FieldAccessor, StatListener, Stats}
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.{EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractCompositeConverter.{CompositeEvaluationContext, PredicateContext}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.metrics.micrometer.utils.TagUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.InputStream
import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

abstract class AbstractCompositeConverter[T <: AnyRef](
    sft: SimpleFeatureType,
    errorMode: ErrorMode,
    delegates: Seq[(Predicate, ParsingConverter[T])]
  ) extends SimpleFeatureConverter with LazyLogging {

  private val tags = TagUtils.typeNameTag(sft.getTypeName)

  private val routingTimer =
    Timer.builder(ConverterMetrics.name("predicate.eval"))
      .tags(tags)
      .publishPercentileHistogram()
      .minimumExpectedValue(Duration.ofNanos(1))
      .maximumExpectedValue(Duration.ofMillis(2))
      .register(Metrics.globalRegistry)

  private val parseTimer =
    Timer.builder(ConverterMetrics.name("parse"))
      .tags(tags)
      .publishPercentileHistogram()
      .minimumExpectedValue(Duration.ofNanos(1))
      .maximumExpectedValue(Duration.ofMillis(10))
      .register(Metrics.globalRegistry)

  protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[T]

  override def targetSft: SimpleFeatureType = sft

  override def createEvaluationContext(globalParams: Map[String, Any]): EvaluationContext =
    createEvaluationContext(delegates.map(_._2.createEvaluationContext(globalParams)), Stats(tags))

  override def createEvaluationContext(globalParams: Map[String, Any], success: Counter, failure: Counter): EvaluationContext =
    createEvaluationContext(delegates.map(_._2.createEvaluationContext(globalParams, success, failure)), Stats.wrap(success, failure, tags))

  private def createEvaluationContext(contexts: Seq[EvaluationContext], theseStats: Stats): EvaluationContext = {
    val stats = new Stats() {
      override def success(i: Int): Long = theseStats.success(i) + contexts.map(_.stats.success(0)).sum
      override def failure(i: Int): Long = theseStats.failure(i) + contexts.map(_.stats.failure(0)).sum
    }
    CompositeEvaluationContext(contexts, stats)
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

    def eval(element: T): CloseableIterator[SimpleFeature] = {
      val start = System.nanoTime()
      args(0) = element
      val converted = predicates.collectFirst { case p if matches(p.predicate) =>
        routingTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS) // note: invoke before executing the conversion
        p.context.line = ec.line // update the line in the sub context
        p.converter.convert(CloseableIterator.single(element), p.context)
      }
      converted.getOrElse {
        routingTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        ec.stats.failure()
        CloseableIterator.empty
      }
    }

    new ErrorHandlingIterator(parse(is, ec), errorMode, ec, parseTimer).flatMap(eval)
  }

  override def close(): Unit = CloseWithLogging(delegates.map(_._2))
}

object AbstractCompositeConverter {

  case class CompositeEvaluationContext(contexts: Seq[EvaluationContext], stats: Stats) extends EvaluationContext {
    private val errs = new java.util.ArrayDeque[EvaluationError]()
    override def errors: java.util.Queue[EvaluationError] = {
      contexts.foreach { c =>
        val iter = c.errors.iterator()
        while (iter.hasNext) {
          errs.add(iter.next())
          iter.remove()
        }
      }
      errs
    }
    override def withListener(listener: ContextListener): EvaluationContext =
      CompositeEvaluationContext(contexts.map(_.withListener(listener)), StatListener(stats, listener))

    override val success: com.codahale.metrics.Counter = new com.codahale.metrics.Counter() {
      override def inc(n: Long): Unit = stats.success(n.toInt)
      override def getCount: Long = stats.success(0)
    }
    override val failure: com.codahale.metrics.Counter = new com.codahale.metrics.Counter() {
      override def inc(n: Long): Unit = stats.failure(n.toInt)
      override def getCount: Long = stats.failure(0)
    }
    override def cache: Map[String, EnrichmentCache] = throw new UnsupportedOperationException()
    override def accessor(name: String): FieldAccessor = throw new UnsupportedOperationException()
    override def evaluate(args: Array[AnyRef]): Either[EvaluationError, Array[AnyRef]] =
      throw new UnsupportedOperationException()
  }

  case class PredicateContext[T <: SimpleFeatureConverter](
      predicate: Predicate,
      converter: T,
      context: EvaluationContext
    )
}
