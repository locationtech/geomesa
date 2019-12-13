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
class XZ2SFCTest extends Specification with LazyLogging {

  val sfc = XZ2SFC(12)

  "XZ2" should {
    "index polygons and query them" >> {
      val poly = sfc.index(10, 10, 12, 12)

      val containing = Seq(
        (9.0, 9.0, 13.0, 13.0),
        (-180.0, -90.0, 180.0, 90.0),
        (0.0, 0.0, 180.0, 90.0),
        (0.0, 0.0, 20.0, 20.0)
      )
      val overlapping = Seq(
        (11.0, 11.0, 13.0, 13.0),
        (9.0, 9.0, 11.0, 11.0),
        (10.5, 10.5, 11.5, 11.5),
        (11.0, 11.0, 11.0, 11.0)
      )
      // note: in general, some disjoint ranges will match due to false positives
      val disjoint = Seq(
        (-180.0, -90.0, 8.0, 8.0),
        (0.0, 0.0, 8.0, 8.0),
        (9.0, 9.0, 9.5, 9.5),
        (20.0, 20.0, 180.0, 90.0)
      )
      forall(containing ++ overlapping) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (!matches) {
          logger.warn(s"$bbox - no match")
        }
        matches must beTrue
      }
      forall(disjoint) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (matches) {
          logger.warn(s"$bbox - invalid match")
        }
        matches must beFalse
      }
    }

    "index points and query them" >> {
      val poly = sfc.index(11, 11, 11, 11)

      val containing = Seq(
        (9.0, 9.0, 13.0, 13.0),
        (-180.0, -90.0, 180.0, 90.0),
        (0.0, 0.0, 180.0, 90.0),
        (0.0, 0.0, 20.0, 20.0)
      )
      val overlapping = Seq(
        (11.0, 11.0, 13.0, 13.0),
        (9.0, 9.0, 11.0, 11.0),
        (10.5, 10.5, 11.5, 11.5),
        (11.0, 11.0, 11.0, 11.0)
      )
      // note: in general, some disjoint ranges will match due to false positives
      val disjoint = Seq(
        (-180.0, -90.0, 8.0, 8.0),
        (0.0, 0.0, 8.0, 8.0),
        (9.0, 9.0, 9.5, 9.5),
        (12.5, 12.5, 13.5, 13.5),
        (20.0, 20.0, 180.0, 90.0)
      )
      forall(containing ++ overlapping) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (!matches) {
          logger.warn(s"$bbox - no match")
        }
        matches must beTrue
      }
      forall(disjoint) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
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

      val ranges = sfc.ranges(45.0, 23.0, 48.0, 27.0)
      forall(geoms) { geom =>
        val index = sfc.index(geom)
        val matches = ranges.exists(r => r.lower <= index && r.upper >= index)
        if (!matches) {
          logger.warn(s"$geom - no match")
        }
        matches must beTrue
      }
    }

    "fail for out-of-bounds values" >> {
      val toFail = Seq(
        (-180.1, 0d, -179.9, 1d),
        (179.9, 0d, 180.1, 1d),
        (-180.3, 0d, -180.1, 1d),
        (180.1, 0d, 180.3, 1d),
        (-180.1, 0d, 180.1, 1d),
        (0d, -90.1, 1d, -89.9),
        (0d, 89.9, 1d, 90.1),
        (0d, -90.3, 1d, -90.1),
        (0d, 90.1, 1d, 90.3),
        (0d, -90.1, 1d, 90.1),
        (-181d, -91d, 0d, 0d),
        (0d, 0d, 181d, 91d)
      )
      forall(toFail) { case (xmin, ymin, xmax, ymax) =>
        sfc.index(xmin, ymin, xmax, ymax) must throwAn[IllegalArgumentException]
      }
    }
  }
}
