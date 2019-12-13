/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import org.locationtech.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.MinMax.MinMaxGeometry
import org.locationtech.geomesa.utils.stats.{Histogram, Stat}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatsHistogramCommandTest extends Specification {

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

      println(StatsHistogramCommand.geomHistToString("geom", histogram))

      ok
    }
  }
}
