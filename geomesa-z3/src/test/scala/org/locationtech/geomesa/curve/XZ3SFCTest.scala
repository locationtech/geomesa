/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class XZ3SFCTest extends Specification with LazyLogging {

  val sfc = XZ3SFC(12, TimePeriod.Week)

  "XZ3" should {
    "index polygons and query them" >> {
      val poly = sfc.index(10, 10, 1000, 12, 12, 1000)

      val containing = Seq(
        (9.0, 9.0, 900.0, 13.0, 13.0, 1100.0),
        (-180.0, -90.0, 900.0, 180.0, 90.0, 1100.0),
        (0.0, 0.0, 900.0, 180.0, 90.0, 1100.0),
        (0.0, 0.0, 900.0, 20.0, 20.0, 1100.0)
      )
      val overlapping = Seq(
        (11.0, 11.0, 900.0, 13.0, 13.0, 1100.0),
        (9.0, 9.0, 900.0, 11.0, 11.0, 1100.0),
        (10.5, 10.5, 900.0, 11.5, 11.5, 1100.0),
        (11.0, 11.0, 900.0, 11.0, 11.0, 1100.0)
      )
      // note: in general, some disjoint ranges will match due to false positives
      val disjoint = Seq(
        (-180.0, -90.0, 900.0, 8.0, 8.0, 1100.0),
        (0.0, 0.0, 900.0, 8.0, 8.0, 1100.0),
        (9.0, 9.0, 900.0, 9.5, 9.5, 1100.0),
        (20.0, 20.0, 900.0, 180.0, 90.0, 1100.0)
      )
      forall(containing ++ overlapping) { bbox =>
        val ranges = sfc.ranges(bbox, Some(10000)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (!matches) {
          logger.warn(s"$bbox - no match")
        }
        matches must beTrue
      }
      forall(disjoint) { bbox =>
        val ranges = sfc.ranges(bbox, Some(10000)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (matches) {
          logger.warn(s"$bbox - invalid match")
        }
        matches must beFalse
      }
    }

    "index points and query them" >> {
      val poly = sfc.index(11, 11, 1000, 11, 11, 1000)

      val containing = Seq(
        (9.0, 9.0, 900.0, 13.0, 13.0, 1100.0),
        (-180.0, -90.0, 900.0, 180.0, 90.0, 1100.0),
        (0.0, 0.0, 900.0, 180.0, 90.0, 1100.0),
        (0.0, 0.0, 900.0, 20.0, 20.0, 1100.0)
      )
      val overlapping = Seq(
        (11.0, 11.0, 900.0, 13.0, 13.0, 1100.0),
        (9.0, 9.0, 900.0, 11.0, 11.0, 1100.0),
        (10.5, 10.5, 900.0, 11.5, 11.5, 1100.0),
        (11.0, 11.0, 900.0, 11.0, 11.0, 1100.0)
      )
      // note: in general, some disjoint ranges will match due to false positives
      val disjoint = Seq(
        (-180.0, -90.0, 900.0, 8.0, 8.0, 1100.0),
        (0.0, 0.0, 900.0, 8.0, 8.0, 1100.0),
        (9.0, 9.0, 900.0, 9.5, 9.5, 1100.0),
        (20.0, 20.0, 900.0, 180.0, 90.0, 1100.0)
      )
      forall(containing ++ overlapping) { bbox =>
        val ranges = sfc.ranges(bbox, Some(10000)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (!matches) {
          logger.warn(s"$bbox - no match")
        }
        matches must beTrue
      }
      forall(disjoint) { bbox =>
        val ranges = sfc.ranges(bbox, Some(10000)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (matches) {
          logger.warn(s"$bbox - invalid match")
        }
        matches must beFalse
      }
    }

    "index complex features and query them2" >> {
      // geometries taken from accumulo FilterTest
      val r = """\((\d+\.\d*),(\d+\.\d*),(\d+\.\d*),(\d+\.\d*)\)""".r
      val source = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("geoms.list"))
      val geoms = try {
        source.getLines.toArray.flatMap { l =>
          r.findFirstMatchIn(l).map { m =>
            (m.group(1).toDouble, m.group(2).toDouble, m.group(3).toDouble, m.group(4).toDouble)
          }
        }
      } finally {
        source.close()
      }

      val ranges = sfc.ranges(45.0, 23.0, 900.0, 48.0, 27.0, 1100.0, Some(10000))
      forall(geoms) { geom =>
        val index = sfc.index(geom._1, geom._2, 1000.0, geom._3, geom._4, 1000.0)
        val matches = ranges.exists(r => r.lower <= index && r.upper >= index)
        if (!matches) {
          logger.warn(s"$geom - no match")
        }
        matches must beTrue
      }
    }

    "fail for out-of-bounds values" >> {
      val tmin = 0d
      val tmax = BinnedTime.maxOffset(TimePeriod.Week).toDouble
      val toFail = Seq(
        (-180.1, 0d, 0d, -179.9, 1d, 1d),
        (179.9, 0d, 0d, 180.1, 1d, 1d),
        (-180.3, 0d, 0d, -180.1, 1d, 1d),
        (180.1, 0d, 0d, 180.3, 1d, 1d),
        (-180.1, 0d, 0d, 180.1, 1d, 1d),
        (0d, -90.1, 0d, 1d, -89.9, 1d),
        (0d, 89.9, 0d, 1d, 90.1, 1d),
        (0d, -90.3, 0d, 1d, -90.1, 1d),
        (0d, 90.1, 0d, 1d, 90.3, 1d),
        (0d, -90.1, 0d, 1d, 90.1, 1d),
        (0d, 0d, tmin - 0.1, 1d, 1d, tmin + 0.1),
        (0d, 0d, tmax - 0.1, 1d, 1d, tmax + 0.1),
        (0d, 0d, tmin - 0.3, 1d, 1d, tmin - 0.1),
        (0d, 0d, tmax + 0.1, 1d, 1d, tmax + 0.3),
        (0d, 0d, tmin - 0.1, 1d, 1d, tmax + 0.1),
        (-181d, -91d, tmin - 1, 0d, 0d, 0d),
        (0d, 0d, 0d, 181d, 91d, tmax + 1)
      )
      forall(toFail) { case (xmin, ymin, zmin, xmax, ymax, zmax) =>
        sfc.index(xmin, ymin, zmin, xmax, ymax, zmax) must throwAn[IllegalArgumentException]
      }
    }
  }
}
