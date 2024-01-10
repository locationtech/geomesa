/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.filter.andFilters
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.time.chrono.ChronoZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}
import java.util.Date
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification with LazyLogging {

  val gf = JTSFactoryFinder.getGeometryFactory

  def dt2lit(dt: ZonedDateTime): Expression = ff.literal(Date.from(dt.toInstant))

  def dts2lit(start: ZonedDateTime, end: ZonedDateTime): Expression = ff.literal(
    new DefaultPeriod(
      new DefaultInstant(new DefaultPosition(Date.from(start.toInstant))),
      new DefaultInstant(new DefaultPosition(Date.from(end.toInstant)))
    ))

  val min: ZonedDateTime = null
  val max: ZonedDateTime = null
  val a   = ZonedDateTime.of(2010,  1, 31, 23, 59, 59, 0, ZoneOffset.UTC)
  val b   = ZonedDateTime.of(2010,  3,  4, 10, 11, 12, 0, ZoneOffset.UTC)
  val c   = ZonedDateTime.of(2011,  2, 12, 15, 34, 23, 0, ZoneOffset.UTC)
  val d   = ZonedDateTime.of(2012, 11,  5,  5, 55, 11, 0, ZoneOffset.UTC)

  val dts = Seq(a, b, c, d)
  val dtPairs: Seq[(ZonedDateTime, ZonedDateTime)] = dts.combinations(2).map(sortDates).toSeq
  val dtAndDtPairs = for( dt <- dts; dtPair <- dtPairs) yield (dt, dtPair)

  val dtFieldName = "dtg"
  val dtp = ff.property(dtFieldName)

  def fAfterDate(dt: ZonedDateTime): Filter = ff.after(dtp, dt2lit(dt))
  def fDateAfter(dt: ZonedDateTime): Filter = ff.after(dt2lit(dt), dtp)
  def fBeforeDate(dt: ZonedDateTime): Filter = ff.before(dtp, dt2lit(dt))
  def fDateBefore(dt: ZonedDateTime): Filter = ff.before(dt2lit(dt), dtp)

  def fLTDate(dt: ZonedDateTime): Filter = ff.less(dtp, dt2lit(dt))
  def fDateLT(dt: ZonedDateTime): Filter = ff.less(dt2lit(dt), dtp)
  def fGTDate(dt: ZonedDateTime): Filter = ff.greater(dtp, dt2lit(dt))
  def fDateGT(dt: ZonedDateTime): Filter = ff.greater(dt2lit(dt), dtp)
  def fLEDate(dt: ZonedDateTime): Filter = ff.lessOrEqual(dtp, dt2lit(dt))
  def fDateLE(dt: ZonedDateTime): Filter = ff.lessOrEqual(dt2lit(dt), dtp)
  def fGEDate(dt: ZonedDateTime): Filter = ff.greaterOrEqual(dtp, dt2lit(dt))
  def fDateGE(dt: ZonedDateTime): Filter = ff.greaterOrEqual(dt2lit(dt), dtp)

  def during(dt1: ZonedDateTime, dt2: ZonedDateTime): Filter = ff.during(dtp, dts2lit(dt1, dt2))
  def during(dtTuple: (ZonedDateTime, ZonedDateTime)): Filter = during(dtTuple._1, dtTuple._2)

  def between(dt1: ZonedDateTime, dt2: ZonedDateTime): Filter = ff.between(dtp, dt2lit(dt1), dt2lit(dt2))
  def between(dtTuple: (ZonedDateTime, ZonedDateTime)): Filter = between(dtTuple._1, dtTuple._2)

  sealed trait DateBound extends Comparable[DateBound] {
    def dt: ZonedDateTime
    def inclusive: Boolean
  }

  case class LowerBound(dt: ZonedDateTime, inclusive: Boolean) extends DateBound {
    override def compareTo(o: DateBound): Int =  o match {
      case UnboundedLower => 1
      case UnboundedUpper => -1
      case _: UpperBound => -1
      case LowerBound(b, i) =>
        dt.compareTo(b) match {
          case 0 if inclusive == i => 0
          case 0 if inclusive => -1
          case 0 => 1
          case i => i
        }
    }
  }
  case class UpperBound(dt: ZonedDateTime, inclusive: Boolean) extends DateBound {
    override def compareTo(o: DateBound): Int = o match {
      case UnboundedLower => 1
      case UnboundedUpper => -1
      case _: LowerBound => 1
      case UpperBound(b, i) =>
        dt.compareTo(b) match {
          case 0 if inclusive == i => 0
          case 0 if inclusive => 1
          case 0 => -1
          case i => i
        }
    }
  }
  case object UnboundedLower extends DateBound {
    override val dt: ZonedDateTime = ZonedDateTime.ofLocal(LocalDateTime.MIN, ZoneOffset.UTC, ZoneOffset.UTC)
    override val inclusive: Boolean = true
    override def compareTo(o: DateBound): Int = o match {
      case UnboundedLower => 0
      case _ => -1
    }
  }
  case object UnboundedUpper extends DateBound {
    override val dt: ZonedDateTime = ZonedDateTime.ofLocal(LocalDateTime.MAX, ZoneOffset.UTC, ZoneOffset.UTC)
    override val inclusive: Boolean = true
    override def compareTo(o: DateBound): Int = o match {
      case UnboundedUpper => 0
      case _ => 1
    }
  }

  case class DateBounds(lower: DateBound, upper: DateBound)

  def afterInterval(dt: ZonedDateTime, inclusive: Boolean = false): DateBounds =
    DateBounds(LowerBound(dt, inclusive), UnboundedUpper)
  def beforeInterval(dt: ZonedDateTime, inclusive: Boolean = false): DateBounds =
    DateBounds(UnboundedLower, UpperBound(dt, inclusive))
  def betweenInterval(dtTuple: (ZonedDateTime, ZonedDateTime)) =
    DateBounds(LowerBound(dtTuple._1, inclusive = true), UpperBound(dtTuple._2, inclusive = true))
  def duringInterval(dtTuple: (ZonedDateTime, ZonedDateTime)) =
    DateBounds(LowerBound(dtTuple._1, inclusive = false), UpperBound(dtTuple._2, inclusive = false))

  def extractDT(f: Seq[Filter], handleExclusiveBounds: Boolean = false): Option[DateBounds] = {
    val extracted = extractIntervals(andFilters(f), dtFieldName, handleExclusiveBounds = handleExclusiveBounds)
    if (extracted.disjoint) { None } else {
      extracted.values must haveLength(1)
      extracted.values.headOption.map { b =>
        val lo = b.lower.value.map(v => LowerBound(v, b.lower.inclusive)).getOrElse(UnboundedLower)
        val hi = b.upper.value.map(v => UpperBound(v, b.upper.inclusive)).getOrElse(UnboundedUpper)
        DateBounds(lo, hi)
      }
    }
  }

  def extractDateTime(fs: String): Option[DateBounds] = {
    val filter = ECQL.toFilter(fs)
    val filters = org.locationtech.geomesa.filter.decomposeAnd(filter)
    extractDT(filters)
  }

  def sortDates(dates: Seq[ZonedDateTime]): (ZonedDateTime, ZonedDateTime) = {
    val Seq(start, end) = dates.sorted(Ordering.ordered[ChronoZonedDateTime[_]])
    (start, end)
  }

  def overlap(i1: DateBounds, i2: DateBounds): Option[DateBounds] = {
    val lower = if (i1.lower.compareTo(i2.lower) >= 0) { i1.lower } else { i2.lower }
    val upper = if (i2.upper.compareTo(i1.upper) <= 0) { i2.upper } else { i1.upper }
    if (lower.dt.isAfter(upper.dt) || (lower.dt == upper.dt && (!lower.inclusive || !upper.inclusive))) {
      None
    } else {
      Some(DateBounds(lower, upper))
    }
  }

  "extractTemporal " should {
    "return 0000 to date for all Before-date and date-After filters" in {
      forall(dts) { dt =>
        val expectedInclusiveInterval = beforeInterval(dt, inclusive = true)
        val expectedExclusiveInterval = beforeInterval(dt, inclusive = false)
        extractDT(Seq(fBeforeDate(dt))) must beSome(expectedExclusiveInterval)
        extractDT(Seq(fDateAfter(dt)))  must beSome(expectedExclusiveInterval)
        extractDT(Seq(fLTDate(dt)))     must beSome(expectedExclusiveInterval)
        extractDT(Seq(fLEDate(dt)))     must beSome(expectedInclusiveInterval)
        extractDT(Seq(fDateGT(dt)))     must beSome(expectedExclusiveInterval)
        extractDT(Seq(fDateGE(dt)))     must beSome(expectedInclusiveInterval)
      }
    }

    "return date to 9999 for After-date and date-Before filters" in {
      forall(dts) { dt =>
        val expectedInclusiveInterval = afterInterval(dt, inclusive = true)
        val expectedExclusiveInterval = afterInterval(dt, inclusive = false)
        extractDT(Seq(fDateBefore(dt))) must beSome(expectedExclusiveInterval)
        extractDT(Seq(fAfterDate(dt)))  must beSome(expectedExclusiveInterval)
        extractDT(Seq(fDateLT(dt)))     must beSome(expectedExclusiveInterval)
        extractDT(Seq(fDateLE(dt)))     must beSome(expectedInclusiveInterval)
        extractDT(Seq(fGTDate(dt)))     must beSome(expectedExclusiveInterval)
        extractDT(Seq(fGEDate(dt)))     must beSome(expectedInclusiveInterval)
      }
    }

    "return date1 to date2 for during filters" in {
      forall(dts.combinations(2).map(sortDates).toSeq) { case (start, end) =>
        val filter = during(start, end)
        val extractedInterval = extractDT(Seq(filter))
        val expectedInterval = duringInterval(start -> end)
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must beSome(expectedInterval)
      }
    }

    "offset dates for during filters" in {
      forall(dts.combinations(2).map(sortDates).toSeq) { case (start, end) =>
        val filter = during(start, end)
        val extractedInterval = extractDT(Seq(filter), handleExclusiveBounds = true)
        val expectedInterval = DateBounds(LowerBound(start.plusSeconds(1), inclusive = true), UpperBound(end.minusSeconds(1), inclusive = true))
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must beSome(expectedInterval)
      }
      val r = new Random(-7)
      forall(dts.combinations(2).map(sortDates).toSeq) { case (s, e) =>
        val filter = during(s.plus(r.nextInt(998) + 1, ChronoUnit.MILLIS), e.plus(r.nextInt(998) + 1, ChronoUnit.MILLIS))
        val extractedInterval = extractDT(Seq(filter), handleExclusiveBounds = true)
        val expectedInterval = DateBounds(LowerBound(s.plusSeconds(1), inclusive = true), UpperBound(e, inclusive = true))
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must beSome(expectedInterval)
      }
    }

    "return date1 to date2 for between filters" in {
      forall(dtPairs) { case (start, end) =>
        val filter = between(start, end)
        val extractedInterval = extractDT(Seq(filter))
        val expectedInterval = betweenInterval(start -> end)
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must beSome(expectedInterval)
      }
    }

    "return appropriate interval for 'and'ed between/during filters" in {
      forall(dtPairs.combinations(2).toSeq) { dtTuples =>
        val Seq(t1, t2) = dtTuples
        val betweenFilters = Seq(between(t1), between(t2))
        val duringFilters = Seq(during(t1), during(t2))
        val mixedFilters1 = Seq(during(t1), between(t2))
        val mixedFilters2 = Seq(between(t1), during(t2))

        val expectedBetween = overlap(betweenInterval(t1), betweenInterval(t2))
        val expectedDuring = overlap(duringInterval(t1), duringInterval(t2))
        val expectedMix1 = overlap(duringInterval(t1), betweenInterval(t2))
        val expectedMix2 = overlap(betweenInterval(t1), duringInterval(t2))

        val extractedBetweenInterval = extractDT(betweenFilters)
        val extractedDuringInterval = extractDT(duringFilters)
        val extractedMixed1Interval = extractDT(mixedFilters1)
        val extractedMixed2Interval = extractDT(mixedFilters2)

        logger.debug(s"Extracted interval $extractedBetweenInterval from filters ${betweenFilters.map(ECQL.toCQL)}")
        extractedBetweenInterval mustEqual expectedBetween
        extractedDuringInterval mustEqual expectedDuring
        extractedMixed1Interval mustEqual expectedMix1
        extractedMixed2Interval mustEqual expectedMix2
      }
    }

    "return appropriate interval for 'and's of before/after and between/during filters" in {
      forall(dtAndDtPairs) { case (dt, dtPair) =>
        val afterDtFilter = fAfterDate(dt)
        val beforeDtFilter = fBeforeDate(dt)

        val afterDtInterval = afterInterval(dt)
        val beforeDtInterval = beforeInterval(dt)

        val betweenFilter = between(dtPair)
        val duringFilter = during(dtPair)
        val betweenDtInterval = betweenInterval(dtPair)
        val duringDtInterval = duringInterval(dtPair)

        val afterAndBetween = extractDT(Seq(afterDtFilter, betweenFilter))
        val afterAndBetweenInterval = overlap(afterDtInterval, betweenDtInterval)
        afterAndBetween must equalTo(afterAndBetweenInterval)

        val beforeAndBetween = extractDT(Seq(beforeDtFilter, betweenFilter))
        val beforeAndBetweenInterval = overlap(beforeDtInterval, betweenDtInterval)
        beforeAndBetween must equalTo(beforeAndBetweenInterval)

        val afterAndDuring = extractDT(Seq(afterDtFilter, duringFilter))
        val afterAndDuringInterval = overlap(afterDtInterval, duringDtInterval)
        afterAndDuring must equalTo(afterAndDuringInterval)

        val beforeAndDuring = extractDT(Seq(beforeDtFilter, duringFilter))
        val beforeAndDuringInterval = overlap(beforeDtInterval, duringDtInterval)
        beforeAndDuring must equalTo(beforeAndDuringInterval)
      }
    }
  }

  "filterListAsAnd as an inverse of decomposeAnd" should {
    "handle empty sequences" in {
     val emptyFilterSeq = Seq[Filter]()
     val filteredSeq = filterListAsAnd(emptyFilterSeq)
      filteredSeq must beNone
    }

    "handle sequences with just one entry" in {
      val processed = baseFilters.flatMap{filter => filterListAsAnd(org.locationtech.geomesa.filter.decomposeAnd(filter))}
      val difference = processed diff baseFilters
      difference must beEmpty
    }
  }
}
