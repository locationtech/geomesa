/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter._
import org.geotools.api.filter.expression.{Expression, Literal, PropertyName}
import org.geotools.api.filter.temporal.{After, Before, During, TEquals}
import org.geotools.api.temporal.{Instant, Period}
import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionRange
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionKey
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.ReceiptTimeScheme.BufferingFilterVisitor
import org.locationtech.geomesa.utils.geotools.converters.FastConverter

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

/**
 * Scheme for partitioning based on "receipt time", i.e. when a message is received. Generally this is useful
 * only for reading existing data that may have been aggregated and stored by an external process.
 *
 * @param buffer amount of time to buffer queries by, in order to match a feature date to a receipt time date -
 *               i.e. the amount of latency in the ingest process
 */
class ReceiptTimeScheme(
    formatter: DateTimeFormatter,
    pattern: String,
    stepUnit: ChronoUnit,
    step: Int,
    dtg: String,
    dtgIndex: Int,
    val buffer: Duration,
  ) extends HierarchicalDateTimeScheme(formatter, pattern, stepUnit, step, dtg, dtgIndex) {

  import ReceiptTimeScheme.Config

  override val name: String =
    HierarchicalDateTimeScheme(formatter, pattern, stepUnit, step, dtg, dtgIndex).name
      .replaceFirst(HierarchicalDateTimeScheme.Name, ReceiptTimeScheme.Name) + s":${Config.BufferOpt}=${buffer.toMillis}ms"

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] =
    super.getRangesForFilter(buffered(filter))

  override def getCoveringFilter(partition: PartitionKey): Filter =
    throw new UnsupportedOperationException("Dates may overlap in multiple partitions")

  private def buffered(filter: Filter): Filter =
    filter.accept(new BufferingFilterVisitor(buffer, dtg), null).asInstanceOf[Filter]
}

object ReceiptTimeScheme {

  val Name = "receipt-time"

  object Config {
    val DateTimeSchemaOpt: String = "datetime-scheme"
    val BufferOpt        : String = "buffer"
  }

  class ReceiptTimePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      if (opts.name != Name) { None } else {
        val buffer = opts.opts.get(Config.BufferOpt).map(Duration.apply).getOrElse(Duration(30, TimeUnit.MINUTES))
        val dateTimeName = opts.opts.getOrElse(Config.DateTimeSchemaOpt, HierarchicalDateTimeScheme.Name)
        val dt = HierarchicalDateTimeScheme.load(sft, s"$dateTimeName:${scheme.drop(Name.length + 1)}").getOrElse {
          throw new IllegalArgumentException(s"Could not load HierarchicalDateTimeScheme from provided options: $scheme")
        }
        Some(new ReceiptTimeScheme(dt.formatter, dt.pattern, dt.stepUnit, dt.step, dt.dtg, dt.dtgIndex, buffer))
      }
    }
  }

  /**
   * Buffers any filters against the specified date attribute by the amount specified
   *
   * @param buffer amount of time to buffer (on each side of) a temporal filter
   * @param dtg date attribute
   */
  class BufferingFilterVisitor(buffer: Duration, dtg: String) extends DuplicatingFilterVisitor {

    private var inverted = false
    private val millis = buffer.toMillis

    override def visit(filter: PropertyIsBetween, extraData: AnyRef): AnyRef = {
      val factory = getFactory(extraData)
      def buffer(p: PropertyName, lower: Literal, upper: Literal): Option[Filter] = {
        for {
          lo <- Option(FastConverter.evaluate(lower, classOf[Date]))
          up <- Option(FastConverter.evaluate(upper, classOf[Date]))
        } yield {
          val bufferedLo = bufferDown(lo)
          val bufferedUp = bufferUp(up)
          // account for inverted filters that may result in invalid clauses after buffering
          if (bufferedLo.before(bufferedUp)) {
            factory.between(p, factory.literal(bufferedLo), factory.literal(bufferedUp), filter.getMatchAction)
          } else {
            Filter.EXCLUDE
          }
        }
      }
      val prop = visit(filter.getExpression, extraData)
      val lowerBoundary = visit(filter.getLowerBoundary, extraData)
      val upperBoundary = visit(filter.getUpperBoundary, extraData)
      val buffered = (prop, lowerBoundary, upperBoundary) match {
        case (p: PropertyName, lower: Literal, upper: Literal) if p.getPropertyName == dtg => buffer(p, lower, upper)
        case _ => None
      }

      buffered.getOrElse(super.visit(filter, extraData))
    }

    override def visit(filter: PropertyIsEqualTo, extraData: AnyRef): AnyRef = {
      def buffer(p: PropertyName, lit: Literal): Filter =
        getFactory(extraData).between(p, bufferDown(lit, extraData), bufferUp(lit, extraData), filter.getMatchAction)
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg => buffer(p, lit)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg => buffer(p, lit)
        case _ => super.visit(filter, extraData)
      }
    }

    override def visit(filter: PropertyIsNotEqualTo, extraData: AnyRef): AnyRef = {
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, _: Literal) if p.getPropertyName == dtg => Filter.INCLUDE
        case (_: Literal, p: PropertyName) if p.getPropertyName == dtg => Filter.INCLUDE
        case _ => super.visit(filter, extraData)
      }
    }

    override def visit(filter: PropertyIsGreaterThan, extraData: AnyRef): AnyRef = {
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg =>
          getFactory(extraData).greater(p, bufferDown(lit, extraData), false, filter.getMatchAction)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg =>
          getFactory(extraData).greater(bufferUp(lit, extraData), p, false, filter.getMatchAction)
        case _ =>
          super.visit(filter, extraData)
      }
    }

    override def visit(filter: PropertyIsGreaterThanOrEqualTo, extraData: AnyRef): AnyRef = {
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg =>
          getFactory(extraData).greaterOrEqual(p, bufferDown(lit, extraData), false, filter.getMatchAction)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg =>
          getFactory(extraData).greaterOrEqual(bufferUp(lit, extraData), p, false, filter.getMatchAction)
        case _ =>
          super.visit(filter, extraData)
      }
    }

    override def visit(filter: PropertyIsLessThan, extraData: AnyRef): AnyRef = {
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg =>
          getFactory(extraData).less(p, bufferUp(lit, extraData), false, filter.getMatchAction)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg =>
          getFactory(extraData).less(bufferDown(lit, extraData), p, false, filter.getMatchAction)
        case _ =>
          super.visit(filter, extraData)
      }
    }

    override def visit(filter: PropertyIsLessThanOrEqualTo, extraData: AnyRef): AnyRef = {
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg =>
          getFactory(extraData).lessOrEqual(p, bufferUp(lit, extraData), false, filter.getMatchAction)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg =>
          getFactory(extraData).lessOrEqual(bufferDown(lit, extraData), p, false, filter.getMatchAction)
        case _ =>
          super.visit(filter, extraData)
      }
    }

    override def visit(filter: After, extraData: AnyRef): AnyRef = {
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg =>
          getFactory(extraData).after(p, bufferDown(lit, extraData), filter.getMatchAction)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg =>
          getFactory(extraData).after(bufferUp(lit, extraData), p, filter.getMatchAction)
        case _ =>
          super.visit(filter, extraData)
      }
    }

    override def visit(filter: Before, extraData: AnyRef): AnyRef = {
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg =>
          getFactory(extraData).before(p, bufferUp(lit, extraData), filter.getMatchAction)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg =>
          getFactory(extraData).before(bufferDown(lit, extraData), p, filter.getMatchAction)
        case _ =>
          super.visit(filter, extraData)
      }
    }

    override def visit(filter: During, extraData: AnyRef): AnyRef = {
      val factory = getFactory(extraData)
      def instant(date: Date): Instant = new DefaultInstant(new DefaultPosition(date))
      def buffer(p: PropertyName, lit: Literal): Option[Filter] = {
        for {
          period   <- Option(FastConverter.evaluate(lit, classOf[Period]))
          lowerPos <- Option(period.getBeginning.getPosition)
          upperPos <- Option(period.getEnding.getPosition)
          lower    <- Option(lowerPos.getDate)
          upper    <- Option(upperPos.getDate)
        } yield {
          val low = bufferDown(lower)
          val up = bufferUp(upper)
          // account for inverted filters that may result in invalid clauses after buffering
          if (low.before(up)) {
            factory.during(p, factory.literal(new DefaultPeriod(instant(low), instant(up))), filter.getMatchAction)
          } else {
            Filter.EXCLUDE
          }
        }
      }
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      val buffered = (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg => buffer(p, lit)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg => buffer(p, lit)
        case _ => None
      }
      buffered.getOrElse(super.visit(filter, extraData))
    }

    override def visit(filter: TEquals, extraData: AnyRef): AnyRef = {
      def buffer(p: PropertyName, lit: Literal): Filter =
        getFactory(extraData).between(p, bufferDown(lit, extraData), bufferUp(lit, extraData), filter.getMatchAction)
      val expr1 = visit(filter.getExpression1, extraData)
      val expr2 = visit(filter.getExpression2, extraData)
      (expr1, expr2) match {
        case (p: PropertyName, lit: Literal) if p.getPropertyName == dtg => buffer(p, lit)
        case (lit: Literal, p: PropertyName) if p.getPropertyName == dtg => buffer(p, lit)
        case _ => super.visit(filter, extraData)
      }
    }

    override def visit(filter: Not, extraData: AnyRef): AnyRef = {
      inverted = !inverted
      val res = try { filter.getFilter.accept(this, extraData).asInstanceOf[Filter] } finally {
        inverted = !inverted
      }
      getFactory(extraData).not(res)
    }

    private def bufferUp(lit: Literal, extraData: AnyRef): Expression =
      buffer(lit, if (inverted) { -1L * millis } else { millis }, extraData)

    private def bufferUp(date: Date): Date =
      buffer(date, if (inverted) { -1L * millis } else { millis })

    private def bufferDown(lit: Literal, extraData: AnyRef): Expression =
      buffer(lit, if (inverted) { millis } else { -1L * millis }, extraData)

    private def bufferDown(date: Date): Date =
      buffer(date, if (inverted) { millis } else { -1L * millis })

    private def buffer(lit: Literal, offset: Long, extraData: AnyRef): Expression = {
      FastConverter.convert(lit.evaluate(null), classOf[Date]) match {
        case null => lit
        case date => getFactory(extraData).literal(buffer(date, offset))
      }
    }

    private def buffer(date: Date, offset: Long): Date = new Date(date.getTime + offset)
  }
}
