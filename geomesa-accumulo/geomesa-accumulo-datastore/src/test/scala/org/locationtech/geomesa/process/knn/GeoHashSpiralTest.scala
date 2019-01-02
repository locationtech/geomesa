/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.knn

import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoHashSpiralTest extends Specification {

  def generateCvilleSF = {
    val sftName = "geomesaKNNTestQueryFeature"

    val sft = SimpleFeatureTypes.createType(sftName, "geom:Point:srid=4326,dtg:Date,dtg_end_time:Date")

    val cvilleSF = SimpleFeatureBuilder.build(sft, List(), "charlottesville")
    cvilleSF.setDefaultGeometry(WKTUtils.read(f"POINT(-78.4953560 38.0752150 )"))
    cvilleSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    cvilleSF
  }

  def generateLineSF = {
    val sftName = "geomesaKNNTestQueryFeature"

    val sft = SimpleFeatureTypes.createType(sftName, "geom:LineString:srid=4326,dtg:Date,dtg_end_time:Date")

    val lineSF = SimpleFeatureBuilder.build(sft, List(), "route 29")
    lineSF.setDefaultGeometry(WKTUtils.read(f"LINESTRING(-78.491 38.062, -78.474 38.082)"))
    lineSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    lineSF
  }

  "Geomesa GeoHashSpiral PriorityQueue" should {

    "order GeoHashes correctly around Charlottesville" in {
      val cvilleSF = generateCvilleSF

      val cvillePQ = GeoHashSpiral(cvilleSF, 500.0, 5000.0)

      val cvillePQ2List = cvillePQ.toList

      val nearest9ByCalculation = cvillePQ2List.take(9).map{_.hash}

      // the below are ordered by geodetic distances
      val nearest9ByVisualInspection = List (
      "dqb0tg",
      "dqb0te",
      "dqb0tf",
      "dqb0td",
      "dqb0tu",
      "dqb0ts",
      "dqb0w5",
      "dqb0w4",
      "dqb0tc")


      nearest9ByCalculation must equalTo(nearest9ByVisualInspection)
    }

    "use the statefulDistanceFilter around Charlottesville correctly before pulling GeoHashes" in {
      val cvilleSF = generateCvilleSF

      val cvillePQ = GeoHashSpiral(cvilleSF, 500.0, 10000.0)
      cvillePQ.mutateFilterDistance(1000.0)  // units are meters

      val numHashesAfterFilter = cvillePQ.toList.length

      numHashesAfterFilter must equalTo(12)
    }

    "use the statefulDistanceFilter around Charlottesville correctly after pulling GeoHashes " in {
      val cvilleSF = generateCvilleSF

      val cvillePQ = GeoHashSpiral(cvilleSF, 500.0, 10000.0)

      // take the 20 closest GeoHashes
      val ghBeforeFilter = cvillePQ.take(20)

      ghBeforeFilter.length must equalTo(20)

      // now mutate the filter -- this is restrictive enough that no further GeoHashes should pass
      cvillePQ.mutateFilterDistance(1000.0)  // units are meters

      // attempt to take five more
      val ghAfterFilter =  cvillePQ.take(5)

      ghAfterFilter.length must equalTo(0)
    }

    "throw an exception if given a non-point geometry"  in {
       val route29SF = generateLineSF

       GeoHashSpiral(route29SF, 500.0, 10000.0) should throwAn[RuntimeException]
    }
  }
}
