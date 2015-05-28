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

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.data.{Range => AccRange}
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CoveringAttributeIndexTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "name:String:index=full,age:Integer:index=join,weight:Double:index=true," +
      "height:Double,dtg:Date,*geom:Geometry:srid=4326"

  val geom = WKTUtils.read("POINT(45.0 49.0)")

  addFeatures({
    (0 until 10).map { i =>
      val dtg = s"2014-01-1${i}T12:00:00.000Z"
      val attrs = Array(s"${i}name$i", s"$i", s"${i * 2.0}", s"${i * 3.0}", dtg, geom)
      ScalaSimpleFeatureFactory.buildFeature(sft, attrs, i.toString)
    }
  })

  "AttributeIndexStrategy" should {

    "support full coverage of attributes" in {
      val query = new Query(sftName, ECQL.toFilter("name = '3name3'"))
      explain(query).indexOf("Using record join iterator") mustEqual(-1)

      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features()).toList

      features must haveSize(1)
      features(0).getAttribute("name") mustEqual("3name3")
      features(0).getAttribute("age") mustEqual(3)
      features(0).getAttribute("weight") mustEqual(6.0)
      features(0).getAttribute("height") mustEqual(9.0)
      features(0).getAttribute("dtg").toString must contain("Jan 13")
    }

    "support transforms in fully covered indices" in {
      val query = new Query(sftName, ECQL.toFilter("name = '3name3'"), Array("name", "age", "dtg", "geom"))
      explain(query).indexOf("Using record join iterator") mustEqual(-1)

      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features()).toList

      features must haveSize(1)
      features(0).getAttribute("name") mustEqual("3name3")
      features(0).getAttribute("age") mustEqual(3)
      features(0).getAttribute("weight") must beNull
      features(0).getAttribute("height") must beNull
      features(0).getAttribute("dtg").toString must contain("Jan 13")
    }

    "support ecql filters in fully covered indices" in {
      val query = new Query(sftName, ECQL.toFilter("name >= '3name3' AND height = '9.0'"))
      explain(query).indexOf("Using record join iterator") mustEqual(-1)

      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features()).toList

      features must haveSize(1)
      features(0).getAttribute("name") mustEqual("3name3")
      features(0).getAttribute("age") mustEqual(3)
      features(0).getAttribute("weight") mustEqual(6.0)
      features(0).getAttribute("height") mustEqual(9.0)
      features(0).getAttribute("dtg").toString must contain("Jan 13")
    }

    "support ecql filters and covering transforms in fully covered indices" in {
      val query = new Query(sftName, ECQL.toFilter("name >= '3name3' AND height = '9.0'"),
        Array("name", "height", "dtg", "geom"))
      explain(query).indexOf("Using record join iterator") mustEqual(-1)

      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features()).toList

      features must haveSize(1)
      features(0).getAttribute("name") mustEqual("3name3")
      features(0).getAttribute("age") must beNull
      features(0).getAttribute("weight") must beNull
      features(0).getAttribute("height") mustEqual(9.0)
      features(0).getAttribute("dtg").toString must contain("Jan 13")
    }

    "support ecql filters and non-covering transforms in fully covered indices" in {
      val query = new Query(sftName, ECQL.toFilter("name >= '3name3' AND height = '9.0'"),
        Array("name", "age", "dtg", "geom"))
      explain(query).indexOf("Using record join iterator") mustEqual(-1)

      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features()).toList

      features must haveSize(1)
      features(0).getAttribute("name") mustEqual("3name3")
      features(0).getAttribute("age") mustEqual(3)
      features(0).getAttribute("weight") must beNull
      features(0).getAttribute("height") must beNull
      features(0).getAttribute("dtg").toString must contain("Jan 13")
    }

    "support join coverage of attributes" in {
      val query = new Query(sftName, ECQL.toFilter("age = '5'"))
      explain(query).indexOf("Using record join iterator") must beGreaterThan(-1)

      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features()).toList

      features must haveSize(1)
      features(0).getAttribute("name") mustEqual("5name5")
      features(0).getAttribute("age") mustEqual(5)
      features(0).getAttribute("weight") mustEqual(10.0)
      features(0).getAttribute("height") mustEqual(15.0)
      features(0).getAttribute("dtg").toString must contain("Jan 15")
    }

    "be backwards compatible with index spec" in {
      val query = new Query(sftName, ECQL.toFilter("weight = '4.0'"))
      explain(query).indexOf("Using record join iterator") must beGreaterThan(-1)

      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features()).toList

      features must haveSize(1)
      features(0).getAttribute("name") mustEqual("2name2")
      features(0).getAttribute("age") mustEqual(2)
      features(0).getAttribute("weight") mustEqual(4.0)
      features(0).getAttribute("height") mustEqual(6.0)
      features(0).getAttribute("dtg").toString must contain("Jan 12")
    }
  }
}
