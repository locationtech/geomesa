package org.locationtech.geomesa.core.index

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.filter.TestFilters._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.utils.filters.Filters._
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification with Logging {
  val ff = CommonFactoryFinder.getFilterFactory2

  val min = IndexSchema.minDateTime
  val max = IndexSchema.maxDateTime
  val a   = new DateTime(2010,  1, 31, 23, 59, 59, DateTimeZone.forID("UTC"))
  val b   = new DateTime(2010,  3,  4, 10, 11, 12, DateTimeZone.forID("UTC"))
  val c   = new DateTime(2011,  2, 12, 15, 34, 23, DateTimeZone.forID("UTC"))
  val d   = new DateTime(2012, 11,  5,  5, 55, 11, DateTimeZone.forID("UTC"))

  val dts = Seq(a, b, c, d)
  val dtPairs: Seq[(DateTime, DateTime)] = dts.combinations(2).map(sortDates).toSeq
  val dtAndDtPairs = for( dt <- dts; dtPair <- dtPairs) yield (dt, dtPair)

  val dtp = ff.property("dtg")

  def before(dt: DateTime): Filter = ff.before(dtp, dt2lit(dt))
  def after(dt: DateTime): Filter = ff.after(dtp, dt2lit(dt))

  def during(dt1: DateTime, dt2: DateTime): Filter = ff.during(dtp, dts2lit(dt1, dt2))
  def during(dtTuple: (DateTime, DateTime)): Filter = during(dtTuple._1, dtTuple._2)

  def between(dt1: DateTime, dt2: DateTime): Filter = ff.between(dtp, dt2lit(dt1), dt2lit(dt2))
  def between(dtTuple: (DateTime, DateTime)): Filter = between(dtTuple._1, dtTuple._2)

  def interval(dtTuple: (DateTime, DateTime)) = new Interval(dtTuple._1, dtTuple._2)
  def afterInterval(dt: DateTime): Interval   = new Interval(dt, max)
  def beforeInterval(dt: DateTime): Interval  = new Interval(min, dt)

  def extractInterval(fs: String): Interval = {
    val filter = ECQL.toFilter(fs)

    val filters = decomposeAnd(filter)
    extractTemporal(filters)
  }

  def sortDates(dates: Seq[DateTime]): (DateTime, DateTime) = {
    val sorted = dates.sortBy(_.getMillis)
    val start = sorted(0)
    val end = sorted(1)
    (start, end)
  }

  "extractTemporal " should {
    "return 0000 to date for Before filters" in {
      forall(dts) { dt =>
        val extractedInterval = extractTemporal(Seq(before(dt)))
        val expectedInterval = beforeInterval(dt)
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "return date to 9999 for After filters" in {
      forall(dts) { dt =>
        val filter = after(dt)
        val extractedInterval = extractTemporal(Seq(filter))
        val expectedInterval = afterInterval(dt)
        println(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "return date1 to date2 for during filters" in {
      forall(dts.combinations(2).map(sortDates)) { case (start, end) =>

        val filter = during(start, end)

        val extractedInterval = extractTemporal(Seq(filter))
        val expectedInterval = new Interval(start, end)
        println(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
        extractedInterval must equalTo(expectedInterval)
      }
    }

    "return date1 to date2 for between filters" in {
      forall(dtPairs) { case (start, end) =>

        val filter = between(start, end)

        val extractedInterval = extractTemporal(Seq(filter))
        val expectedInterval = new Interval(start, end)
        println(s"Extracted interval $extractedInterval from filter ${ECQL.toCQL(filter)}")
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

        val extractedBetweenInterval = extractTemporal(betweenFilters)
        val extractedDuringInterval = extractTemporal(duringFilters)
        val extractedMixed1Interval = extractTemporal(mixedFilters1)
        val extractedMixed2Interval = extractTemporal(mixedFilters2)

        val expectedInterval = interval(t1).overlap(interval(t2))
        println(s"Extracted interval $extractedBetweenInterval from filters ${betweenFilters.map(ECQL.toCQL)}")
        extractedBetweenInterval must equalTo(expectedInterval)
        extractedDuringInterval must equalTo(expectedInterval)
        extractedMixed1Interval must equalTo(expectedInterval)
        extractedMixed2Interval must equalTo(expectedInterval)
      }
    }

    "return appropriate interval for 'and's of before/after and between/during filters" in {
      forall(dtAndDtPairs) { case (dt, dtPair) =>
        val afterDtFilter = after(dt)
        val beforeDtFilter = before(dt)

        val afterDtInterval = afterInterval(dt)
        val beforeDtInterval = beforeInterval(dt)

        val betweenFilter = between(dtPair)
        val duringFilter = during(dtPair)
        val pairInterval = interval(dtPair)

        val afterAndBetween = extractTemporal(Seq(afterDtFilter, betweenFilter))
        val afterAndBetweenInterval = afterDtInterval.overlap(pairInterval)
        afterAndBetween must equalTo(afterAndBetweenInterval)

        val beforeAndBetween = extractTemporal(Seq(beforeDtFilter, betweenFilter))
        val beforeAndBetweenInterval = beforeDtInterval.overlap(pairInterval)
        beforeAndBetween must equalTo(beforeAndBetweenInterval)

        val afterAndDuring = extractTemporal(Seq(afterDtFilter, duringFilter))
        val afterAndDuringInterval = afterDtInterval.overlap(pairInterval)
        afterAndDuring must equalTo(afterAndDuringInterval)

        val beforeAndDuring = extractTemporal(Seq(beforeDtFilter, duringFilter))
        val beforeAndDuringInterval = beforeDtInterval.overlap(pairInterval)
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
