/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.filter.Bounds
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.FilterHelper._
import org.opengis.filter.expression.Expression
import org.opengis.filter.{And, Filter}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification with LazyLogging {

  import org.locationtech.geomesa.filter.ff

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

  def interval(dtTuple: (ZonedDateTime, ZonedDateTime)) = (dtTuple._1, dtTuple._2)
  def afterInterval(dt: ZonedDateTime): (ZonedDateTime, ZonedDateTime)  = (dt, max)
  def beforeInterval(dt: ZonedDateTime): (ZonedDateTime, ZonedDateTime) = (min, dt)

  val extractDT: (Seq[Filter]) => (ZonedDateTime, ZonedDateTime) = {
    import scala.collection.JavaConversions._
    (f) => extractIntervals(ff.and(f), dtFieldName).values.headOption
        .map(b => (b.lower.value.orNull, b.upper.value.orNull))
        .getOrElse(null, null)
  }

  def decomposeAnd(f: Filter): Seq[Filter] = {
    import scala.collection.JavaConversions._
    f match {
      case b: And => b.getChildren.toSeq.flatMap(decomposeAnd)
      case f: Filter => Seq(f)
    }
  }

  def extractDateTime(fs: String): (ZonedDateTime, ZonedDateTime) = {
    val filter = ECQL.toFilter(fs)
    val filters = decomposeAnd(filter)
    extractDT(filters)
  }

  def sortDates(dates: Seq[ZonedDateTime]): (ZonedDateTime, ZonedDateTime) = {
    val sorted = dates.sortBy(d => (d.toEpochSecond * 1000) + (d.getNano / 1000000))
    val start = sorted(0)
    val end = sorted(1)
    (start, end)
  }

  "extractTemporal " should {
    "return 0000 to date for all Before-date and date-After filters" in {
      forall(dts) { dt =>
        val expectedInterval = beforeInterval(dt)
        extractDT(Seq(fBeforeDate(dt))) must equalTo(expectedInterval)
        extractDT(Seq(fDateAfter(dt)))  must equalTo(expectedInterval)
        extractDT(Seq(fLTDate(dt)))     must equalTo(expectedInterval)
        extractDT(Seq(fLEDate(dt)))     must equalTo(expectedInterval)
        extractDT(Seq(fDateGT(dt)))     must equalTo(expectedInterval)
        extractDT(Seq(fDateGE(dt)))     must equalTo(expectedInterval)
      }
    }

    "return date to 9999 for After-date and date-Before filters" in {
      forall(dts) { dt =>
        val expectedInterval = afterInterval(dt)
        extractDT(Seq(fDateBefore(dt))) must equalTo(expectedInterval)
        extractDT(Seq(fAfterDate(dt)))  must equalTo(expectedInterval)
        extractDT(Seq(fDateLT(dt)))     must equalTo(expectedInterval)
        extractDT(Seq(fDateLE(dt)))     must equalTo(expectedInterval)
        extractDT(Seq(fGTDate(dt)))     must equalTo(expectedInterval)
        extractDT(Seq(fGEDate(dt)))     must equalTo(expectedInterval)
      }
    }

    "return date to 9999 for date-Before filters" in {
      forall(dts) { dt =>
        val extractedInterval = extractDT(Seq(fDateBefore(dt)))
        val expectedInterval = afterInterval(dt)
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "return date1 to date2 for during filters" in {
      forall(dts.combinations(2).map(sortDates).toSeq) { case (start, end) =>

        val filter = during(start, end)

        val extractedInterval = extractDT(Seq(filter))
        val expectedInterval = (start, end)
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "offset dates for during filters" in {
      forall(dts.combinations(2).map(sortDates).toSeq) { case (start, end) =>
        val filter = during(start, end)
        val extractedInterval = extractIntervals(filter, dtFieldName, handleExclusiveBounds = true).values.head
        val expectedInterval = Bounds(Bound(Some(start.plusSeconds(1)), inclusive = true), Bound(Some(end.minusSeconds(1)), inclusive = true))
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
      val r = new Random(-7)
      forall(dts.combinations(2).map(sortDates).toSeq) { case (s, e) =>
        val start = s.plus(r.nextInt(998) + 1, ChronoUnit.MILLIS)
        val end = e.plus(r.nextInt(998) + 1, ChronoUnit.MILLIS)
        val filter = during(start, end)
        val extractedInterval = extractIntervals(filter, dtFieldName, handleExclusiveBounds = true).values.head
        val expectedInterval = Bounds(Bound(Some(s.plusSeconds(1)), inclusive = true), Bound(Some(e), inclusive = true))
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "return date1 to date2 for between filters" in {
      forall(dtPairs) { case (start, end) =>

        val filter = between(start, end)

        val extractedInterval = extractDT(Seq(filter))
        val expectedInterval = (start, end)
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "return appropriate interval for 'and'ed between/during filters" in {
      forall(dtPairs.combinations(2).toSeq) { dtTuples =>
        val t1 = dtTuples(0)
        val t2 = dtTuples(1)

        val betweenFilters = Seq(between(t1), between(t2))
        val duringFilters = Seq(during(t1), during(t2))
        val mixedFilters1 = Seq(during(t1), between(t2))
        val mixedFilters2 = Seq(between(t1), during(t2))

        val extractedBetweenInterval = extractDT(betweenFilters)
        val extractedDuringInterval = extractDT(duringFilters)
        val extractedMixed1Interval = extractDT(mixedFilters1)
        val extractedMixed2Interval = extractDT(mixedFilters2)

        val expectedStart = math.max(t1._1.toInstant.toEpochMilli, t2._1.toInstant.toEpochMilli)
        val expectedEnd = math.min(t1._2.toInstant.toEpochMilli, t2._2.toInstant.toEpochMilli)
        val expectedInterval = if (expectedStart > expectedEnd) (null, null) else
          (ZonedDateTime.ofInstant(Instant.ofEpochMilli(expectedStart), ZoneOffset.UTC),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(expectedEnd), ZoneOffset.UTC))
        logger.debug(s"Extracted interval $extractedBetweenInterval from filters ${betweenFilters.map(ECQL.toCQL)}")
        extractedBetweenInterval mustEqual expectedInterval
        extractedDuringInterval mustEqual expectedInterval
        extractedMixed1Interval mustEqual expectedInterval
        extractedMixed2Interval mustEqual expectedInterval
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
        val pairInterval = interval(dtPair)

        def overlap(i1: (ZonedDateTime, ZonedDateTime), i2: (ZonedDateTime, ZonedDateTime)) = {
          val s = math.max(Option(i1._1).map(_.toInstant.toEpochMilli).getOrElse(0L), Option(i2._1).map(_.toInstant.toEpochMilli).getOrElse(0L))
          val e = math.min(Option(i1._2).map(_.toInstant.toEpochMilli).getOrElse(Long.MaxValue), Option(i2._2).map(_.toInstant.toEpochMilli).getOrElse(Long.MaxValue))
          if (s > e) (null, null) else (ZonedDateTime.ofInstant(Instant.ofEpochMilli(s), ZoneOffset.UTC), ZonedDateTime.ofInstant(Instant.ofEpochMilli(e), ZoneOffset.UTC))
        }
        val afterAndBetween = extractDT(Seq(afterDtFilter, betweenFilter))
        val afterAndBetweenInterval = overlap(afterDtInterval, pairInterval)
        afterAndBetween must equalTo(afterAndBetweenInterval)

        val beforeAndBetween = extractDT(Seq(beforeDtFilter, betweenFilter))
        val beforeAndBetweenInterval = overlap(beforeDtInterval, pairInterval)
        beforeAndBetween must equalTo(beforeAndBetweenInterval)

        val afterAndDuring = extractDT(Seq(afterDtFilter, duringFilter))
        val afterAndDuringInterval = overlap(afterDtInterval, pairInterval)
        afterAndDuring must equalTo(afterAndDuringInterval)

        val beforeAndDuring = extractDT(Seq(beforeDtFilter, duringFilter))
        val beforeAndDuringInterval = overlap(beforeDtInterval, pairInterval)
        beforeAndDuring must equalTo(beforeAndDuringInterval)
      }
    }
  }

  "filterListAsAnd as an inverse of decomposeAnd" should {
    "handle empty sequences" in {
     val emptyFilterSeq = Seq[Filter]()
     val filteredSeq = filterListAsAnd(emptyFilterSeq)

      filteredSeq.isDefined must beFalse
    }

    "handle sequences with just one entry" in {
      val processed = baseFilters.flatMap{filter => filterListAsAnd(decomposeAnd(filter))}
      val difference = processed diff baseFilters

      difference.isEmpty must beTrue
    }
  }
}
