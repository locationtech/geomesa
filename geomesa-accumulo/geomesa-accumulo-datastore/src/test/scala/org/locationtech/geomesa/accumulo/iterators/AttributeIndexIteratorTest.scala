/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.accumulo.iterators

import java.text.SimpleDateFormat
import java.util.{Collections, Date, TimeZone}

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.Query
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToWrite
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.AttributeTable
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.features.{SimpleFeatureSerializers, SimpleFeatureSerializer}
import org.locationtech.geomesa.utils.geotools.{Conversions, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AttributeIndexIteratorTest extends Specification with TestWithDataStore {

  val spec = "name:String:index=true,age:Integer:index=true,scars:List[String]:index=true,dtg:Date:index=true,*geom:Geometry:srid=4326"

  val dateToIndex = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))
    sdf.parse("20140102")
  }

  addFeatures({
    List("a", "b", "c", "d", null).flatMap { name =>
      List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
        val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
        sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
        sf.setAttribute("dtg", dateToIndex)
        sf.setAttribute("age", i)
        sf.setAttribute("name", name)
        sf.setAttribute("scars", Collections.singletonList("face"))
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        sf
      }
    }
  })

  val ff = CommonFactoryFinder.getFilterFactory2

  "AttributeIndexIterator" should {

    "implement the Accumulo iterator stack properly" in {
      import Conversions._
      val table = "AttributeIndexIteratorTest_2"
      connector.tableOperations.create(table, true, TimeType.LOGICAL)

      val bw = connector.createBatchWriter(table, GeoMesaBatchWriterConfig())
      val attributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).zipWithIndex
      val indexValueEncoder = IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION)
      val featureEncoder = SimpleFeatureSerializers(sft, DEFAULT_ENCODING)
      val rowIdPrefix = org.locationtech.geomesa.accumulo.index.getTableSharingPrefix(sft)

      fs.getFeatures().features().foreach { feature =>
        val toWrite = new FeatureToWrite(feature, "", featureEncoder, indexValueEncoder)
        val muts = AttributeTable.getAttributeIndexMutations(toWrite, attributes, rowIdPrefix)
        bw.addMutations(muts)
      }
      bw.close()

      // Scan and retrieve type = b manually with the iterator
      val scanner = connector.createScanner(table, new Authorizations())
      val opts = Map[String, String](
        GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE -> "dtg:Date,*geom:Geometry:srid=4326",
        GEOMESA_ITERATORS_SFT_NAME -> sftName,
        GEOMESA_ITERATORS_SFT_INDEX_VALUE -> spec,
        GEOMESA_ITERATORS_VERSION -> INTERNAL_GEOMESA_VERSION.toString
      )
      val is = new IteratorSetting(40, classOf[AttributeIndexIterator], opts)
      scanner.addScanIterator(is)
      val range = AttributeTable.getAttributeIndexRows(rowIdPrefix, sft.getDescriptor("name"), "b").head
      scanner.setRange(new ARange(range))
      scanner.iterator.size mustEqual 4
    }

    def checkExplainStrategy(filter: String) = {
      val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
      val explain = new ExplainString()
      ds.explainQuery(sftName, query, explain)
      val output = explain.toString()
      val iter = output.split("\n").filter(_.startsWith("Iterators")).headOption
      iter must beSome
      iter.get must contain(classOf[AttributeIndexIterator].getName)
    }

    "be selected for appropriate queries" in {
      val filters = List(
        "name = 'b'",
        "name < 'b'",
        "name > 'b'",
        "dtg TEQUALS 2014-01-01T12:30:00.000Z",
        "dtg = '2014-01-01T12:30:00.000Z'",
        "dtg BETWEEN '2012-01-01T12:00:00.000Z' AND '2013-01-01T12:00:00.000Z'",
        "age < 10"
      )
      forall(filters) { filter => checkExplainStrategy(filter) }
    }

    "be selected for appropriate queries" in {
      val filters = Seq("name = 'b' AND dtg = '2014-01-01T12:30:00.000Z'")
      filters.foreach(checkExplainStrategy)
      success
    }.pendingUntilFixed("GEOMESA-394 AttributeIndexIterator should be able to handle attribute + date queries")

    "not be selected for inappropriate queries" in {
      val filters = Map(
        "name = 'b' AND age = 3" -> None,
        "scars = 'face'" -> Some("scars")
      )
      filters.foreach { case (filter, prop) =>
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg") ++ prop)
        val explain = new ExplainString()
        ds.explainQuery(sftName, query, explain)
        val output = explain.toString().split("\n")
        output.forall(s => !s.startsWith("AttributeIndexIterator:")) must beTrue
      }
      success
    }

    "return correct results" >> {

      "for string equals" >> {
        val filter = "name = 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("b").foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for string less than" >> {
        val filter = "name < 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("a").foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for string greater than" >> {
        val filter = "name > 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(8)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("c").exactly(4)
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("d").exactly(4)
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for string greater than or equals" >> {
        val filter = "name >= 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(12)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("b").exactly(4)
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("c").exactly(4)
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("d").exactly(4)
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for date tequals" >> {
        val filter = "dtg TEQUALS 2014-01-02T00:00:00.000Z"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(20)
        results.map(_.getAttributeCount) must contain(2).foreach
      }

      "for date equals" >> {
        val filter = "dtg = '2014-01-02T00:00:00.000Z'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(20)
        results.map(_.getAttributeCount) must contain(2).foreach
      }

      "for date between" >> {
        val filter = "dtg BETWEEN '2014-01-01T00:00:00.000Z' AND '2014-01-03T00:00:00.000Z'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(20)
        results.map(_.getAttributeCount) must contain(2).foreach
      }

      "for int less than" >> {
        val filter = "age < 2"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "age"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(5)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("age").asInstanceOf[Int]) must contain(1).foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)").foreach
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for int greater than or equals" >> {
        val filter = "age >= 3"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "age"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(10)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("age").asInstanceOf[Int]) must contain(3).exactly(5)
        results.map(_.getAttribute("age").asInstanceOf[Int]) must contain(4).exactly(5)
        results.map(_.getAttribute("geom").toString) must contain("POINT (47 47)").exactly(5)
        results.map(_.getAttribute("geom").toString) must contain("POINT (48 48)").exactly(5)
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "not including attribute queried on" >> {
        val filter = "name = 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(2).foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "not including geom" >> {
        val filter = "name = 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("dtg"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(2).foreach // geom gets added back in
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "not including dtg" >> {
        val filter = "name = 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(1).foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
      }

      "not including geom or dtg" >> {
        val filter = "name = 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("name"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(2).foreach // geom gets added back in
        results.map(_.getAttribute("name").toString) must contain("b").foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
      }

      "with additional filter applied" >> {
        val filter = ff.and(ECQL.toFilter("name = 'b'"), ECQL.toFilter("BBOX(geom, 44.5, 44.5, 45.5, 45.5)"))
        val query = new Query(sftName, filter, Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(ds.getFeatureReader(sftName, query)).toList

        results must haveSize(1)
        results.map(_.getAttributeCount) must contain(3).foreach // geom gets added back in
        results.map(_.getAttribute("name").toString) must contain("b")
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)")
      }
    }
  }

}
