/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.CURRENT_SCHEMA_VERSION
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BackCompatibilityTest extends Specification with TestWithMultipleSfts {

  sequential

  val spec = "name:String:index=true:cardinality=high,age:Int,dtg:Date,geom:Point:srid=4326"

  def getTestFeatures(sft: SimpleFeatureType) = (0 until 10).map { i =>
    val name = s"name$i"
    val age = java.lang.Integer.valueOf(10 + i)
    val dtg = s"2014-01-1${i}T00:00:00.000Z"
    val geom = s"POINT(45 5$i)"
    ScalaSimpleFeatureFactory.buildFeature(sft, Array(name, age, dtg, geom), s"$i")
  }

  val queries = Seq(
    ("bbox(geom, 40, 54.5, 50, 60)", Seq(5, 6, 7, 8, 9)),
    ("bbox(geom, 40, 54.5, 50, 60) AND dtg DURING 2014-01-10T00:00:00.000Z/2014-01-17T23:59:59.999Z", Seq(5, 6, 7)),
    ("name = 'name5' AND bbox(geom, 40, 54.5, 50, 60) AND dtg DURING 2014-01-10T00:00:00.000Z/2014-01-17T23:59:59.999Z", Seq(5)),
    ("name = 'name5' AND dtg DURING 2014-01-10T00:00:00.000Z/2014-01-17T23:59:59.999Z", Seq(5)),
    ("name = 'name5' AND bbox(geom, 40, 54.5, 50, 60)", Seq(5)),
    ("age > '16' AND bbox(geom, 40, 54.5, 50, 60)", Seq(7, 8, 9)),
    ("dtg DURING 2014-01-10T00:00:00.000Z/2014-01-17T23:59:59.999Z", Seq(1, 2, 3, 4, 5, 6, 7))
  )

  val transforms = Seq(
    Array("geom"),
    Array("geom", "dtg"),
    Array("geom", "name")
  )

  def doQuery(fs: SimpleFeatureSource, query: Query): Seq[Int] =
    fs.getFeatures(query).features.map(_.getID.toInt).toList

  def runVersionTest(version: Int) = {
    val sft = createNewSchema(spec)
    ds.setGeomesaVersion(sft.getTypeName, version)
    addFeatures(sft, getTestFeatures(sft))

    val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]

    queries.foreach { case (q, results) =>
      val filter = ECQL.toFilter(q)
      doQuery(fs, new Query(sft.getTypeName, filter)) mustEqual results
      transforms.foreach { t =>
        doQuery(fs, new Query(sft.getTypeName, filter, t)) mustEqual results
      }
    }
  }

  "GeoMesa" should {
    (2 until CURRENT_SCHEMA_VERSION).foreach { version =>
      s"support back compatibility to version $version" >> {
        runVersionTest(version)
        success
      }
    }
  }
}
