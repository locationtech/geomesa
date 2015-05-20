/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BackCompatibilityTest extends Specification with TestWithDataStore {

  sequential

  override def spec =
    """
      |name:String:index=true,
      |age:Int,
      |dtg:Date,
      |*geom:Point:srid=4326
    """.stripMargin

  def getTestFeatures() = {
    (0 until 10).map { i =>
      val name = s"name$i"
      val age = java.lang.Integer.valueOf(10 + i)
      val dtg = s"2014-01-1${i}T00:00:00.000Z"
      val geom = s"POINT(45 5$i)"
      ScalaSimpleFeatureFactory.buildFeature(sft, Array(name, age, dtg, geom), s"$i")
    }
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
    ds.removeSchema(sftName)
    ds.createSchema(sft)
    ds.setGeomesaVersion(sftName, version)

    val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    getTestFeatures().foreach { f =>
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(f)
    }
    // write the feature to the store
    fs.addFeatures(featureCollection)

    queries.foreach { case (q, results) =>
      val filter = ECQL.toFilter(q)
      doQuery(fs, new Query(sftName, filter)) mustEqual(results)
      transforms.foreach { t =>
        doQuery(fs, new Query(sftName, filter, t)) mustEqual(results)
      }
    }
  }

  "GeoMesa" should {
    "support back compatibility to version 2" >> {
      runVersionTest(2)
      success
    }

    "support back compatibility to version 3" >> {
      runVersionTest(3)
      success
    }

    "support back compatibility to version 4" >> {
      runVersionTest(4)
      success
    }
  }

}
