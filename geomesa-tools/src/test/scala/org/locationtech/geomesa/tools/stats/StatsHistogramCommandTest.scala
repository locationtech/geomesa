/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.stats.{GeoMesaStats, Stat}
import org.locationtech.geomesa.index.stats.impl.Histogram
import org.locationtech.geomesa.index.stats.impl.MinMax.MinMaxGeometry
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Geometry
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatsHistogramCommandTest extends Specification with LazyLogging {

  "StatsHistogramCommand" should {
    "put points on the map" >> {
      skipped("integration")

      val sft = SimpleFeatureTypes.createType("StatsHistogramCommandTest", "geom:Geometry:srid=4326")
      val length = GeoMesaStats.MaxHistogramSize
      val stat = Stat(sft, Stat.Histogram[Geometry]("geom", length, MinMaxGeometry.min, MinMaxGeometry.max))
      val histogram = stat.asInstanceOf[Histogram[Geometry]]

      def addPoint(x: Int, y: Int): Unit = {
        histogram.observe(new ScalaSimpleFeature(sft, "", Array(WKTUtils.read(s"POINT ($x $y)"))))
        histogram.observe(new ScalaSimpleFeature(sft, "", Array(WKTUtils.read(s"POINT (${x + 0.1} ${y + 0.1})"))))
        histogram.observe(new ScalaSimpleFeature(sft, "", Array(WKTUtils.read(s"POINT (${x - 0.1} ${y + 0.1})"))))
        histogram.observe(new ScalaSimpleFeature(sft, "", Array(WKTUtils.read(s"POINT (${x + 0.1} ${y - 0.1})"))))
        histogram.observe(new ScalaSimpleFeature(sft, "", Array(WKTUtils.read(s"POINT (${x - 0.1} ${y - 0.1})"))))
      }

      addPoint(0, 0)

      addPoint(-179, -89)
      addPoint(-179, 89)
      addPoint(179, -89)
      addPoint(179, 89)

      addPoint(-90, -45)
      addPoint(-90, 45)
      addPoint(90, -45)
      addPoint(90, 45)

      addPoint(-76, 44)

      logger.info(StatsHistogramCommand.geomHistToString("geom", histogram))

      ok
    }
  }
}
