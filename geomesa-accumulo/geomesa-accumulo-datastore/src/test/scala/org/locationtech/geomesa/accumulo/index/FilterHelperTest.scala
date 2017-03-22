/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.utils.filters.Filters._
import org.opengis.filter.{And, Filter}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification with Mockito with LazyLogging {
  val ff = CommonFactoryFinder.getFilterFactory2
  val gf = JTSFactoryFinder.getGeometryFactory

  val min = FilterHelper.MinDateTime
  val max = FilterHelper.MaxDateTime
  val a   = new DateTime(2010,  1, 31, 23, 59, 59, DateTimeZone.UTC)
  val b   = new DateTime(2010,  3,  4, 10, 11, 12, DateTimeZone.UTC)
  val c   = new DateTime(2011,  2, 12, 15, 34, 23, DateTimeZone.UTC)
  val d   = new DateTime(2012, 11,  5,  5, 55, 11, DateTimeZone.UTC)

  val dts = Seq(a, b, c, d)
  val dtPairs: Seq[(DateTime, DateTime)] = dts.combinations(2).map(sortDates).toSeq
  val dtAndDtPairs = for( dt <- dts; dtPair <- dtPairs) yield (dt, dtPair)

  val dtFieldName = "dtg"
  val dtp = ff.property(dtFieldName)

  def fAfterDate(dt: DateTime): Filter = ff.after(dtp, dt2lit(dt))
  def fDateAfter(dt: DateTime): Filter = ff.after(dt2lit(dt), dtp)
  def fBeforeDate(dt: DateTime): Filter = ff.before(dtp, dt2lit(dt))
  def fDateBefore(dt: DateTime): Filter = ff.before(dt2lit(dt), dtp)

  def fLTDate(dt: DateTime): Filter = ff.less(dtp, dt2lit(dt))
  def fDateLT(dt: DateTime): Filter = ff.less(dt2lit(dt), dtp)
  def fGTDate(dt: DateTime): Filter = ff.greater(dtp, dt2lit(dt))
  def fDateGT(dt: DateTime): Filter = ff.greater(dt2lit(dt), dtp)
  def fLEDate(dt: DateTime): Filter = ff.lessOrEqual(dtp, dt2lit(dt))
  def fDateLE(dt: DateTime): Filter = ff.lessOrEqual(dt2lit(dt), dtp)
  def fGEDate(dt: DateTime): Filter = ff.greaterOrEqual(dtp, dt2lit(dt))
  def fDateGE(dt: DateTime): Filter = ff.greaterOrEqual(dt2lit(dt), dtp)

  def during(dt1: DateTime, dt2: DateTime): Filter = ff.during(dtp, dts2lit(dt1, dt2))
  def during(dtTuple: (DateTime, DateTime)): Filter = during(dtTuple._1, dtTuple._2)

  def between(dt1: DateTime, dt2: DateTime): Filter = ff.between(dtp, dt2lit(dt1), dt2lit(dt2))
  def between(dtTuple: (DateTime, DateTime)): Filter = between(dtTuple._1, dtTuple._2)

  def interval(dtTuple: (DateTime, DateTime)) = (dtTuple._1, dtTuple._2)
  def afterInterval(dt: DateTime): (DateTime, DateTime)  = (dt, max)
  def beforeInterval(dt: DateTime): (DateTime, DateTime) = (min, dt)

  val extractDT: (Seq[Filter]) => (DateTime, DateTime) = {
    import scala.collection.JavaConversions._
    (f) => extractIntervals(ff.and(f), dtFieldName).values.headOption.getOrElse(null, null)
  }

  def decomposeAnd(f: Filter): Seq[Filter] = {
    import scala.collection.JavaConversions._
    f match {
      case b: And => b.getChildren.toSeq.flatMap(decomposeAnd)
      case f: Filter => Seq(f)
    }
  }

  def extractDateTime(fs: String): (DateTime, DateTime) = {
    val filter = ECQL.toFilter(fs)
    val filters = decomposeAnd(filter)
    extractDT(filters)
  }

  def sortDates(dates: Seq[DateTime]): (DateTime, DateTime) = {
    val sorted = dates.sortBy(_.getMillis)
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
      forall(dts.combinations(2).map(sortDates)) { case (start, end) =>

        val filter = during(start, end)

        val extractedInterval = extractDT(Seq(filter))
        val expectedInterval = (start, end)
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "offset dates for during filters" in {
      forall(dts.combinations(2).map(sortDates)) { case (start, end) =>
        val filter = during(start, end)
        val extractedInterval = extractIntervals(filter, dtFieldName, handleExclusiveBounds = true).values.head
        val expectedInterval = (start.plusSeconds(1), end.minusSeconds(1))
        logger.debug(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
      val r = new Random(-7)
      forall(dts.combinations(2).map(sortDates)) { case (s, e) =>
        val start = s.plusMillis(r.nextInt(998) + 1)
        val end = e.plusMillis(r.nextInt(998) + 1)
        val filter = during(start, end)
        val extractedInterval = extractIntervals(filter, dtFieldName, handleExclusiveBounds = true).values.head
        val expectedInterval = (s.plusSeconds(1), e)
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
      forall(dtPairs.combinations(2)) { dtTuples =>
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

        val expectedStart = math.max(t1._1.getMillis, t2._1.getMillis)
        val expectedEnd = math.min(t1._2.getMillis, t2._2.getMillis)
        val expectedInterval = if (expectedStart > expectedEnd) (null, null) else
          (new DateTime(expectedStart, DateTimeZone.UTC), new DateTime(expectedEnd, DateTimeZone.UTC))
        logger.debug(s"Extracted interval $extractedBetweenInterval from filters ${betweenFilters.map(ECQL.toCQL)}")
        extractedBetweenInterval must equalTo(expectedInterval)
        extractedDuringInterval must equalTo(expectedInterval)
        extractedMixed1Interval must equalTo(expectedInterval)
        extractedMixed2Interval must equalTo(expectedInterval)
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

        def overlap(i1: (DateTime, DateTime), i2: (DateTime, DateTime)) = {
          val s = math.max(i1._1.getMillis, i2._1.getMillis)
          val e = math.min(i1._2.getMillis, i2._2.getMillis)
          if (s > e) (null, null) else (new DateTime(s, DateTimeZone.UTC), new DateTime(e, DateTimeZone.UTC))
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
