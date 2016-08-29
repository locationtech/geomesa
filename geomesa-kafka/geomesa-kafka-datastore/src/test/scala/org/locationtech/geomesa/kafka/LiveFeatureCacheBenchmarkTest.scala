/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import com.google.common.base.Ticker
import com.vividsolutions.jts.geom.Point
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.joda.time.{DateTime, DateTimeZone, Instant}
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class LiveFeatureCacheBenchmarkTest extends Specification {
  implicit def sfToCreate(feature: SimpleFeature): CreateOrUpdate = CreateOrUpdate(Instant.now, feature)
  implicit val ff = CommonFactoryFinder.getFilterFactory2

  val spec = Seq(
    "Who:String:cq-index=default",
    "What:Integer:cq-index=navigable",
    "When:Date:cq-index=navigable",
    "*Where:Point:srid=4326",
    "Why:String").mkString(",")
  val MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val seconds_per_year = 365L * 24L * 60L * 60L
  val string = "foo"

  def randDate = MIN_DATE.plusSeconds(scala.math.round(scala.util.Random.nextFloat * seconds_per_year)).toDate

  val sft = SimpleFeatureTypes.createType("test", spec)
  val builder = new SimpleFeatureBuilder(sft)

  val names = Array("Addams", "Bierce", "Clemens", "Damon", "Evan", "Fred", "Goliath", "Harry")

  def getName: String = names(Random.nextInt(names.length))

  def getPoint: Point = {
    val minx = -180
    val miny = -90
    val dx = 360
    val dy = 180

    val x = minx + Random.nextDouble * dx
    val y = miny + Random.nextDouble * dy
    WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
  }

  def buildFeature(i: Int): SimpleFeature = {
    builder.set("Who", getName)
    builder.set("What", Random.nextInt(10))
    builder.set("When", randDate)
    builder.set("Where", getPoint)
    if (Random.nextBoolean()) {
      builder.set("Why", string)
    }
    builder.buildFeature(i.toString)
  }

  def mean(values: Seq[Long]): Double = {
    values.sum.toDouble / values.length.toDouble
  }

  def sd(values: Seq[Long]): Double = {
    val mn = mean(values)
    math.sqrt(
      values.map(x => math.pow(x.toDouble - mn, 2.0)).sum /
        (values.length - 1).toDouble)
  }

  def fd(value: Double): String = {
    "%.1f".format(value)
  }

  def runQueries[T](n: Int, genIter: T => Long, filters: Seq[T]) = {
    println(Seq(
      "c_max",
      "c_min",
      "t_max",
      "t_mean",
      "t_sd",
      "t_min",
      "filter"
    ).mkString("\t"))
    for (f <- filters) {
      val timeRes = (1 to n).map(i => time(genIter(f)))
      val counts = timeRes.map(_._1)
      val times = timeRes.map(_._2)
      println(Seq(
        counts.max,
        counts.min,
        times.max,
        fd(mean(times)),
        fd(sd(times)),
        times.min,
        f.toString
      ).mkString("\t"))
    }
  }

  def runQueriesMultiple[T](n: Int,
                            labels: Seq[String],
                            genIter: Seq[T => Long],
                            filters: Seq[T]) = {
    val sep = "\t"
    val header = (for (l <- labels; c <- Seq("count", "tmax", "tmean", "tsd", "tmin")) yield l + "_" + c)
    print(header.mkString(sep))
    println(sep + "filter")

    for (f <- filters) {
      val row = genIter.flatMap {
        g => {
          val timeRes = (1 to n).map(i => time(g(f)))
          val counts = timeRes.map(_._1)
          val times = timeRes.map(_._2)
          Seq(
            counts.max,
            times.max,
            fd(mean(times)),
            fd(sd(times)),
            times.min)
        }
      }
      print(row.mkString(sep))
      println(sep + f)
    }
  }

  def runQueriesMultipleRaw[T](n: Int,
                               labels: Seq[String],
                               genIter: Seq[T => Long],
                               filters: Seq[T]) = {
    val sep = ","
    val header = for (i <- 1 to filters.size ; l <- labels) yield s"f$i.$l"
    println(header.mkString(sep))

    for (i <- 1 to n) {
      val row = for (f <- filters; g <- genIter) yield {
        val (counts, t) = time(g(f))
        t
      }
      println(row.toList.mkString(sep))
    }
  }

  def time[A](a: => A) = {
    val now = System.currentTimeMillis()
    val result = a
    //println("%f seconds".format( (System.nanoTime - now) / 1000000000.0 ))

    (result, System.currentTimeMillis() - now)
  }

  def timeUnit[Unit](a: => Unit) = {
    val now = System.currentTimeMillis()
    a
    System.currentTimeMillis() - now
  }

  val ab = ECQL.toFilter("Who IN('Addams', 'Bierce')")
  val cd = ECQL.toFilter("Who IN('Clemens', 'Damon')")
  val ab_cd = ff.and(ab, cd)

  val w14 = ECQL.toFilter("What = 1 OR What = 2 OR What = 3 or What = 4")

  val where = ECQL.toFilter("BBOX(Where, 0, 0, 180, 90)")
  val where2 = ECQL.toFilter("BBOX(Where, -180, -90, 0, 0)")

  val posIDL = ECQL.toFilter("BBOX(Where, 170, 0, 180, 10)")
  val negIDL = ECQL.toFilter("BBOX(Where, -180, 0, -170, 10)")
  val idl = ff.or(posIDL, negIDL)

  val abIDL = ff.and(idl, ab)

  val posAB = ff.and(posIDL, ab)
  val negAB = ff.and(negIDL, ab)
  val abIDL2 = ff.or(posAB, negAB)

  val bbox2 = ff.or(where, where2)

  val justified = ECQL.toFilter("Why is not null")

  val justifiedAB = ff.and(ff.and(ab, w14), justified)
  val justifiedCD = ff.and(ff.and(cd, w14), justified)

  val just = ff.or(justifiedAB, justifiedCD)

  val justBBOX = ff.and(just, where)
  val justBBOX2 = ff.and(just, where2)

  val justOR = ff.or(justBBOX, justBBOX2)

  val niceAnd = ff.and(justOR, just)

  val niceAnd2 = ff.and(bbox2, just)

  val geoJustAB = ff.and(justifiedAB, bbox2)
  val geoJustCD = ff.and(justifiedCD, bbox2)
  val badOr = ff.or(geoJustAB, geoJustCD)

  val overlapWhere1 = ECQL.toFilter("BBOX(Where, -180, 0, 0, 90)")
  val overlapWhere2 = ECQL.toFilter("BBOX(Where, -90, -90, 0, 90)")
  val overlapOR1 = ff.or(overlapWhere1, overlapWhere2)

  val overlapOR2 = ECQL.toFilter("Who = 'Addams' OR What = 1")

  val overlapORpathological = ff.or(List[Filter](
    "Who = 'Addams'",
    "What = 1",
    "Who = 'Bierce'",
    "What = 2",
    "Who = 'Clemons'",
    "What = 3",
    "Who = 'Damon'",
    "What = 4",
    "Who = 'Evan'",
    "What = 5",
    "Who = 'Fred'",
    "What = 6",
    "Who = 'Goliath'",
    "What = 7",
    "Who = 'Harry'",
    "What = 8"))

  // Easier filters
  val geoCD = ff.and(cd, bbox2)
  val geoAB = ff.and(ab, bbox2)

  val abcd = ff.or(ab, cd)

  val abcdWhere = ff.and(abcd, where)
  val abcdWhere2 = ff.and(abcd, where2)

  val badOr3 = ff.or(geoCD, geoAB)
  val niceAnd3 = ff.and(abcd, bbox2)
  val jor = ff.or(abcdWhere, abcdWhere2)

  // One geom
  val whereAB = ff.and(where, ab)
  val whereCD = ff.and(where, cd)

  val bad1 = ff.or(whereAB, whereCD)
  val nice1 = ff.and(where, abcd)

  val filters = Seq(ab, cd, w14, where, justified, justifiedAB, justifiedCD, just, justBBOX, justBBOX2, bbox2, overlapOR1, overlapOR2, overlapORpathological)

  val nFeats = 100000
  val feats = (0 until nFeats).map(buildFeature)
  val featsUpdate = (0 until nFeats).map(buildFeature)

  // load different LiveFeatureCache implementations
  implicit val ticker = Ticker.systemTicker()
  val lfc = new LiveFeatureCacheGuava(sft, None)
  //val h2  = new LiveFeatureCacheH2(sft)
  val cq = new LiveFeatureCacheCQEngine(sft, None)

  val sfv = new SimplifyingFilterVisitor

  def benchmark(f: Filter) {
    println("Running f")
    val (regularCount, t1) = time(lfc.getReaderForFilter(f).getIterator.size)

    println("\n\nRunning filter in CNF")
    val cnf = rewriteFilterInCNF(f)
    val (cnfCount, tcnf) = time(lfc.getReaderForFilter(cnf.accept(sfv, null).asInstanceOf[Filter]).getIterator.size)

    println("\n\nRunning filter in DNF")
    val dnf = rewriteFilterInDNF(f)
    val (dnfCount, tdnf) = time(lfc.getReaderForFilter(dnf.accept(sfv, null).asInstanceOf[Filter]).getIterator.size)

    println("\n\nRunning filter in 'unoptimized'")
    //val (simpleCount, t2) = time(lfc.getReaderForFilter(f.accept(sfv, null).asInstanceOf[Filter]).getIterator.size)
    val (unoptimizedCount, t3) = time(lfc.unoptimized(f).getIterator.size)

    println(s"\nFilter: $f")
    if (regularCount == cnfCount && regularCount == dnfCount && regularCount == unoptimizedCount) {
      if (t3 < t1) {
        println("'Unoptimized' was quicker than regular")
      }
      if (tcnf < t1) {
        println("'CNF' was quicker than regular")
      }
      if (tdnf < t1) {
        println("'DNF' was quicker than regular")
      }
      println(s"All filters returned $regularCount")

    } else {
      println(s"MISMATCHED Counts: Regular: $regularCount CNF: $cnfCount DNF: $dnfCount  Unoptimized: $unoptimizedCount")
    }
    println(s"Timings: regular: $t1 CNF: $tcnf DNF: $tdnf unoptimized: $t3\n")
  }

  def toC(f: Filter) = {
    f match {
      case or: Or => or.getChildren.toIndexedSeq
      case _ => Seq(f)
    }
  }

  // f is bigger than g
  def contains(f: Filter, g: Filter): Boolean = {
    val fc = toC(f)
    val gc = toC(g)
    fc.contains(gc)
  }


  def printBoolean(f: Filter): String = {
    var int = 0
    val map = new mutable.HashMap[Int, String]()
    val abc = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray

    def printBooleanInternal(f: Filter): String = {
      f match {
        case a: And => "(" + a.getChildren.map(printBooleanInternal).mkString("+") + ")"
        case o: Or => "(" + o.getChildren.map(printBooleanInternal).mkString("*") + ")"
        case _ =>
          println(s"F: ${f.hashCode()} + $map")
          map.get(f.hashCode) match {
            case Some(v) => v
            case None => {
              val ret = abc(int).toString
              println(s"map.put(${f.hashCode()}, $ret)")
              map.put(f.hashCode(), ret)
              int += 1
              ret
            }
          }
      }
    }
    printBooleanInternal(f)
  }

  def countPopulate(count: Int, time: Long): String = {
    "%d in %d ms (%.1f /ms)".format(count, time, count.toDouble / time)
  }

  "LiveFeatureCacheCQEngine " should {
    "benchmark" >> {
      skipped

      val lfc_pop = timeUnit(feats.foreach {
        lfc.createOrUpdateFeature(_)
      })
      println("lfc pop:   " + countPopulate(feats.size, lfc_pop))

      val lfc_repop = timeUnit(featsUpdate.foreach {
        lfc.createOrUpdateFeature(_)
      })
      println("lfc repop: " + countPopulate(featsUpdate.size, lfc_repop))

      val cq_pop = timeUnit({
        for (sf <- feats) cq.createOrUpdateFeature(sf)
      })
      println("cq  pop:   " + countPopulate(feats.size, cq_pop))

      val cq_repop = timeUnit({
        for (sf <- featsUpdate) cq.createOrUpdateFeature(sf)
      })
      println("cq  repop: " + countPopulate(featsUpdate.size, cq_repop))

      runQueriesMultipleRaw[Filter](
        11,
        Seq("lfc", "cq", "cqdd"),
        Seq(
          f => lfc.getReaderForFilter(f).getIterator.size,
          f => cq.geocq.queryCQ(f, false).getIterator.size,
          f => cq.geocq.queryCQ(f, true).getIterator.size),
        filters)

      true must equalTo(true)
    }
  }
}


