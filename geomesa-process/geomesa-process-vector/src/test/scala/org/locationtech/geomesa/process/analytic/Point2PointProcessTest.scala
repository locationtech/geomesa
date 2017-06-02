/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.vividsolutions.jts.geom.{LineString, Point}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Point2PointProcessTest extends Specification {

  val fName = "Point2PointProcess"
  val sft = new SftBuilder().stringType("myid").point("geom", default=true).date("dtg", default = true).build(fName)
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  val points1 = (1 to 5).map( i => WKTUtils.read(s"POINT($i $i)").asInstanceOf[Point]).zip(1 to 5).map { case (p, i) =>
    new ScalaSimpleFeature(s"first$i", sft, Array[AnyRef]("first", p, sdf.parse(s"2015-08-$i")))
  }

  val points2 = (6 to 10).reverse.map( i => WKTUtils.read(s"POINT($i $i)").asInstanceOf[Point]).zip(1 to 5).map { case (p, i) =>
    new ScalaSimpleFeature(s"second$i", sft, Array[AnyRef]("second", p, sdf.parse(s"2015-08-$i")))
  }

  val p2p = new Point2PointProcess

  val features = new ListFeatureCollection(sft)

  step {
    features.addAll(points1 ++ points2)
  }

  "Point2PointProcess" should {
    "properly create linestrings of groups of 2 coordinates" >> {
      import org.locationtech.geomesa.utils.geotools.Conversions._
      val res = p2p.execute(features, "myid", "dtg", 2, breakOnDay = false, filterSingularPoints = true)
      SelfClosingIterator(res.features) must haveLength(8)

      val f1 = SelfClosingIterator(p2p.execute(features.subCollection(ECQL.toFilter("myid = 'first'")),
        "myid", "dtg", 2, breakOnDay = false, filterSingularPoints = true).features).toSeq
      f1.length mustEqual 4

      val f2 = SelfClosingIterator(p2p.execute(features.subCollection(ECQL.toFilter("myid = 'second'")),
        "myid", "dtg", 2, breakOnDay = false, filterSingularPoints = true).features).toSeq
      f2.length mustEqual 4

      f1.forall( sf => sf.getAttributeCount mustEqual 4)
      f1.forall( sf => sf.getDefaultGeometry must beAnInstanceOf[LineString])
      f1.forall( sf => sf.get[String]("myid") mustEqual "first")

      f1.head.lineString.getPointN(0) mustEqual WKTUtils.read("POINT(1 1)")
      f1.head.lineString.getPointN(1) mustEqual WKTUtils.read("POINT(2 2)")
      f1.head.get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-01").getTime
      f1.head.get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-02").getTime

      f1(1).lineString.getPointN(0) mustEqual WKTUtils.read("POINT(2 2)")
      f1(1).lineString.getPointN(1) mustEqual WKTUtils.read("POINT(3 3)")
      f1(1).get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-02").getTime
      f1(1).get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-03").getTime

      f1(2).lineString.getPointN(0) mustEqual WKTUtils.read("POINT(3 3)")
      f1(2).lineString.getPointN(1) mustEqual WKTUtils.read("POINT(4 4)")
      f1(2).get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-03").getTime
      f1(2).get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-04").getTime

      f1(3).lineString.getPointN(0) mustEqual WKTUtils.read("POINT(4 4)")
      f1(3).lineString.getPointN(1) mustEqual WKTUtils.read("POINT(5 5)")
      f1(3).get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-04").getTime
      f1(3).get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-05").getTime

      f2.forall( sf => sf.getAttributeCount mustEqual 4)
      f2.forall( sf => sf.getDefaultGeometry must beAnInstanceOf[LineString])
      f2.forall( sf => sf.get[String]("myid") mustEqual "second")

      f2.head.lineString.getPointN(0) mustEqual WKTUtils.read("POINT(10 10)")
      f2.head.lineString.getPointN(1) mustEqual WKTUtils.read("POINT(9 9)")
      f2.head.get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-01").getTime
      f2.head.get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-02").getTime

      f2(1).lineString.getPointN(0) mustEqual WKTUtils.read("POINT(9 9)")
      f2(1).lineString.getPointN(1) mustEqual WKTUtils.read("POINT(8 8)")
      f2(1).get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-02").getTime
      f2(1).get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-03").getTime

      f2(2).lineString.getPointN(0) mustEqual WKTUtils.read("POINT(8 8)")
      f2(2).lineString.getPointN(1) mustEqual WKTUtils.read("POINT(7 7)")
      f2(2).get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-03").getTime
      f2(2).get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-04").getTime

      f2(3).lineString.getPointN(0) mustEqual WKTUtils.read("POINT(7 7)")
      f2(3).lineString.getPointN(1) mustEqual WKTUtils.read("POINT(6 6)")
      f2(3).get[java.util.Date]("dtg_start").getTime mustEqual sdf.parse("2015-08-04").getTime
      f2(3).get[java.util.Date]("dtg_end").getTime mustEqual sdf.parse("2015-08-05").getTime
    }

    "set the SFT even if the features source passed in is empty" >> {
      val filter = ECQL.toFilter("myid in ('abcdefg_not_here')")
      val res = p2p.execute(features.subCollection(filter),
        "myid", "dtg", 2, breakOnDay = false, filterSingularPoints = true)
      res.getSchema must not(beNull)
      SelfClosingIterator(res.features) must haveLength(0)
    }
  }
}
