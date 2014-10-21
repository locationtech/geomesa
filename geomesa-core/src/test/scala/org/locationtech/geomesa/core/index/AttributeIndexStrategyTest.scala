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

package org.locationtech.geomesa.core.index

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQLException
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.data.{AccumuloDataStore, SimpleFeatureEncoder}
import org.locationtech.geomesa.core.index
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AttributeIndexStrategyTest extends Specification {

  val sftName = "AttributeIndexStrategyTest"
  val spec = "name:String:index=true,age:Integer:index=true,count:Long:index=true," +
      "weight:Double:index=true,height:Float:index=true,admin:Boolean:index=true," +
      "geom:Geometry:srid=4326,dtg:Date:index=true," +
      "fingers:List[String]:index=true,toes:List[Double]:index=true"
  val sft = SimpleFeatureTypes.createType(sftName, spec)
  sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

  val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))

  val ds = DataStoreFinder.getDataStore(Map(
             "connector"   -> connector,
             // note the table needs to be different to prevent testing errors
             "tableName"   -> "AttributeIndexStrategyTest").asJava).asInstanceOf[AccumuloDataStore]

  ds.createSchema(sft, 2)

  val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
  val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
  val dtFormat = new SimpleDateFormat("yyyyMMdd HH:mm:SS")
  dtFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  case class TestAttributes(name: String,
                            age: Integer,
                            count: Long,
                            weight: Double,
                            height: Float,
                            admin: Boolean,
                            geom: Geometry,
                            dtg: Date,
                            fingers: List[String],
                            toes: List[Double])

  val geom = WKTUtils.read("POINT(45.0 49.0)")

  Seq(TestAttributes("alice",   20,   1, 5.0, 10.0F, true,  geom, dtFormat.parse("20120101 12:00:00"),
        List("index"), List(1.0)),
      TestAttributes("bill",    21,   2, 6.0, 11.0F, false, geom, dtFormat.parse("20130101 12:00:00"),
        List("ring", "middle"), List(1.0, 2.0)),
      TestAttributes("bob",     30,   3, 6.0, 12.0F, false, geom, dtFormat.parse("20140101 12:00:00"),
        List("index", "thumb", "pinkie"), List(3.0, 2.0, 5.0)),
      TestAttributes("charles", null, 4, 7.0, 12.0F, false, geom, dtFormat.parse("20140101 12:30:00"),
        List("thumb", "ring", "index", "pinkie", "middle"), List()))
  .foreach { entry =>
    val feature = builder.buildFeature(entry.name)
    feature.setDefaultGeometry(entry.geom)
    feature.setAttributes(entry.productIterator.toArray.asInstanceOf[Array[AnyRef]])

    // make sure we ask the system to re-use the provided feature-ID
    feature.getUserData().asScala(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    featureCollection.add(feature)
  }

  // write the feature to the store
  val fs = ds.getFeatureSource(sftName).asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]
  fs.addFeatures(featureCollection)

  val featureEncoder = SimpleFeatureEncoder(sft, ds.getFeatureEncoding(sft))
  val indexSchema = IndexSchema(ds.getIndexSchemaFmt(sftName), sft, featureEncoder)
  val queryPlanner = indexSchema.planner

  def execute(strategy: AttributeIdxStrategy, filter: String): List[String] = {
    val query = new Query(sftName, ECQL.toFilter(filter))
    val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
    // adapt iterator no longer de-dupes, add dedupe wrapper
    queryPlanner.adaptIterator(results, query).map(_.getAttribute("name").toString).toSet.toList
  }

  "AttributeIndexStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getAttrIdxTableName(sftName), new Authorizations())
      val prefix = AttributeTable.getAttributeIndexRowPrefix(index.getTableSharingPrefix(sft),
        sft.getDescriptor("fingers"))
      scanner.setRange(AccRange.prefix(prefix))
      scanner.asScala.foreach(println)
      println
      success
    }

    "use first indexable attribute if equals" in {
      val strategy = new AttributeIdxEqualsStrategy
      val filter = ECQL.toFilter("age=21 AND count<5")
      val query = new Query(sft.getTypeName, filter)
      val (strippedQuery, extractedFilter) = strategy.partitionFilter(query, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(extractedFilter, sft).get must beAnInstanceOf[AttributeIdxEqualsStrategy]
      val (secondStripped, secondExtractedFilter) = strategy.partitionFilter(strippedQuery, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(secondExtractedFilter, sft).get must beAnInstanceOf[AttributeIdxRangeStrategy]
    }

    "use first indexable attribute if range" in {
      val strategy = new AttributeIdxEqualsStrategy
      val filter = ECQL.toFilter("count<5 AND age=21")
      val query = new Query(sft.getTypeName, filter)
      val (strippedQuery, extractedFilter) = strategy.partitionFilter(query, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(extractedFilter, sft).get must beAnInstanceOf[AttributeIdxRangeStrategy]
      val (secondStripped, secondExtractedFilter) = strategy.partitionFilter(strippedQuery, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(secondExtractedFilter, sft).get must beAnInstanceOf[AttributeIdxEqualsStrategy]
    }

    "use first indexable attribute if like" in {
      val strategy = new AttributeIdxEqualsStrategy
      val filter = ECQL.toFilter("name LIKE 'baddy' AND age=21")
      val query = new Query(sft.getTypeName, filter)
      val (strippedQuery, extractedFilter) = strategy.partitionFilter(query, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(extractedFilter, sft).get must beAnInstanceOf[AttributeIdxLikeStrategy]
      val (secondStripped, secondExtractedFilter) = strategy.partitionFilter(strippedQuery, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(secondExtractedFilter, sft).get must beAnInstanceOf[AttributeIdxEqualsStrategy]
    }

    "use first indexable attribute if like and retain all children for > 2 filters" in {
      val strategy = new AttributeIdxEqualsStrategy
      val filter = FilterHelper.filterListAsAnd(Seq(ECQL.toFilter("name LIKE 'baddy'"), ECQL.toFilter("age=21"), ECQL.toFilter("count<5"))).get
      val query = new Query(sft.getTypeName, filter)
      val (strippedQuery, extractedFilter) = strategy.partitionFilter(query, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(extractedFilter, sft).get must beAnInstanceOf[AttributeIdxLikeStrategy]
      val (secondStripped, secondExtractedFilter) = strategy.partitionFilter(strippedQuery, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(secondExtractedFilter, sft).get must beAnInstanceOf[AttributeIdxEqualsStrategy]
      val (thirdStripped, thirdExtractedFilter) = strategy.partitionFilter(secondStripped, sft)
      AttributeIndexStrategy.getAttributeIndexStrategy(thirdExtractedFilter, sft).get must beAnInstanceOf[AttributeIdxRangeStrategy]
    }

    "all attribute filters should be applied to SFFI" in {
      val strategy = new AttributeIdxLikeStrategy
      val filter = FilterHelper.filterListAsAnd(Seq(ECQL.toFilter("name LIKE 'b%'"), ECQL.toFilter("count<27"), ECQL.toFilter("age<29"))).get
      val query = new Query(sftName, filter)
      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val resultNames = queryPlanner.adaptIterator(results, query).map(_.getAttribute("name").toString).toList
      resultNames must have size(1)
      resultNames must contain ("bill")
    }

    import scala.collection.JavaConversions._

    "there are no attribute filters" in {
      val filter = FilterHelper.filterListAsAnd(Seq(ECQL.toFilter("INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"),
                                                    ECQL.toFilter("INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"))).get
      val (extractedFilter, strippedFilters) = FilterHelper.findFirst(AttributeIndexStrategy.getAttributeIndexStrategy(_, sft).isDefined)(filter.asInstanceOf[org.opengis.filter.And].getChildren)
      extractedFilter must beEmpty
      strippedFilters must have size(2)
    }
  }

  "AttributeIndexEqualsStrategy" should {

    val strategy = new AttributeIdxEqualsStrategy

    "correctly query on ints" in {
      val features = execute(strategy, "age=21")
      features must have size(1)
      features must contain("bill")
    }

    "correctly query on longs" in {
      val features = execute(strategy, "count=2")
      features must have size(1)
      features must contain("bill")
    }

    "correctly query on floats" in {
      val features = execute(strategy, "height=12.0")
      features must have size(2)
      features must contain("bob", "charles")
    }

    "correctly query on floats in different precisions" in {
      val features = execute(strategy, "height=10")
      features must have size(1)
      features must contain("alice")
    }

    "correctly query on doubles" in {
      val features = execute(strategy, "weight=6.0")
      features must have size(2)
      features must contain("bill", "bob")
    }

    "correctly query on doubles in different precisions" in {
      val features = execute(strategy, "weight=6")
      features must have size(2)
      features must contain("bill", "bob")
    }

    "correctly query on booleans" in {
      val features = execute(strategy, "admin=false")
      features must have size(3)
      features must contain("bill", "bob", "charles")
    }

    "correctly query on strings" in {
      val features = execute(strategy, "name='bill'")
      features must have size(1)
      features must contain("bill")
    }

    "correctly query on date objects" in {
      val features = execute(strategy, "dtg TEQUALS 2014-01-01T12:30:00.000Z")
      features must have size(1)
      features must contain("charles")
    }

    "correctly query on date strings in standard format" in {
      val features = execute(strategy, "dtg = '2014-01-01T12:30:00.000Z'")
      features must have size(1)
      features must contain("charles")
    }

    "correctly query on nulls" in {
      val features = execute(strategy, "age is NULL")
      features must have size(1)
      features must contain("charles")
    }

    "correctly query on lists of strings" in {
      val features = execute(strategy, "fingers = 'index'")
      features must have size(3)
      features must contain("alice", "bob", "charles")
    }

    "correctly query on lists of doubles" in {
      val features = execute(strategy, "toes = 2.0")
      features must have size(2)
      features must contain("bill", "bob")
    }
  }

  "AttributeIndexRangeStrategy" should {

    val strategy = new AttributeIdxRangeStrategy

    "correctly query on ints (with nulls)" >> {
      "lt" >> {
        val features = execute(strategy, "age<21")
        features must have size(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute(strategy, "age>21")
        features must have size(1)
        features must contain("bob")
      }
      "lte" >> {
        val features = execute(strategy, "age<=21")
        features must have size(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute(strategy, "age>=21")
        features must have size(2)
        features must contain("bill", "bob")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "age BETWEEN 20 AND 25")
        features must have size(2)
        features must contain("alice", "bill")
      }
    }

    "correctly query on longs" >> {
      "lt" >> {
        val features = execute(strategy, "count<2")
        features must have size(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute(strategy, "count>2")
        features must have size(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute(strategy, "count<=2")
        features must have size(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute(strategy, "count>=2")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "count BETWEEN 3 AND 7")
        features must have size(2)
        features must contain("bob", "charles")
      }
    }

    "correctly query on floats" >> {
      "lt" >> {
        val features = execute(strategy, "height<12.0")
        features must have size(2)
        features must contain("alice", "bill")
      }
      "gt" >> {
        val features = execute(strategy, "height>12.0")
        features must have size(0)
      }
      "lte" >> {
        val features = execute(strategy, "height<=12.0")
        features must have size(4)
        features must contain("alice", "bill", "bob", "charles")
      }
      "gte" >> {
        val features = execute(strategy, "height>=12.0")
        features must have size(2)
        features must contain("bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "height BETWEEN 10.0 AND 11.5")
        features must have size(2)
        features must contain("alice", "bill")
      }
    }

    "correctly query on floats in different precisions" >> {
      "lt" >> {
        val features = execute(strategy, "height<11")
        features must have size(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute(strategy, "height>11")
        features must have size(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute(strategy, "height<=11")
        features must have size(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute(strategy, "height>=11")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "height BETWEEN 11 AND 12")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
    }

    "correctly query on doubles" >> {
      "lt" >> {
        val features = execute(strategy, "weight<6.0")
        features must have size(1)
        features must contain("alice")
      }
      "lt fraction" >> {
        val features = execute(strategy, "weight<6.1")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
      "gt" >> {
        val features = execute(strategy, "weight>6.0")
        features must have size(1)
        features must contain("charles")
      }
      "gt fractions" >> {
        val features = execute(strategy, "weight>5.9")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "lte" >> {
        val features = execute(strategy, "weight<=6.0")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
      "gte" >> {
        val features = execute(strategy, "weight>=6.0")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "weight BETWEEN 5.5 AND 6.5")
        features must have size(2)
        features must contain("bill", "bob")
      }
    }

    "correctly query on doubles in different precisions" >> {
      "lt" >> {
        val features = execute(strategy, "weight<6")
        features must have size(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute(strategy, "weight>6")
        features must have size(1)
        features must contain("charles")
      }
      "lte" >> {
        val features = execute(strategy, "weight<=6")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
      "gte" >> {
        val features = execute(strategy, "weight>=6")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "weight BETWEEN 5 AND 6")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
    }

    "correctly query on strings" >> {
      "lt" >> {
        val features = execute(strategy, "name<'bill'")
        features must have size(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute(strategy, "name>'bill'")
        features must have size(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute(strategy, "name<='bill'")
        features must have size(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute(strategy, "name>='bill'")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "name BETWEEN 'bill' AND 'bob'")
        features must have size(2)
        features must contain("bill", "bob")
      }
    }

    "correctly query on date objects" >> {
      "before" >> {
        val features = execute(strategy, "dtg BEFORE 2014-01-01T12:30:00.000Z")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
      "after" >> {
        val features = execute(strategy, "dtg AFTER 2013-01-01T12:30:00.000Z")
        features must have size(2)
        features must contain("bob", "charles")
      }
      "during (exclusive)" >> {
        val features = execute(strategy, "dtg DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
    }

    "correctly query on date strings in standard format" >> {
      "lt" >> {
        val features = execute(strategy, "dtg < '2014-01-01T12:30:00.000Z'")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
      "gt" >> {
        val features = execute(strategy, "dtg > '2013-01-01T12:00:00.000Z'")
        features must have size(2)
        features must contain("bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "dtg BETWEEN '2012-01-01T12:00:00.000Z' AND '2013-01-01T12:00:00.000Z'")
        features must have size(2)
        features must contain("alice", "bill")
      }
    }

    "correctly query with attribute on right side" >> {
      "lt" >> {
        val features = execute(strategy, "'bill' > name")
        features must have size(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute(strategy, "'bill' < name")
        features must have size(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute(strategy, "'bill' >= name")
        features must have size(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute(strategy, "'bill' <= name")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "before" >> {
        execute(strategy, "2014-01-01T12:30:00.000Z AFTER dtg") should throwA[CQLException]
      }
      "after" >> {
        execute(strategy, "2013-01-01T12:30:00.000Z BEFORE dtg") should throwA[CQLException]
      }
    }

    "correctly query on lists of strings" in {
      "lt" >> {
        val features = execute(strategy, "fingers<'middle'")
        features must have size(3)
        features must contain("alice", "bob", "charles")
      }
      "gt" >> {
        val features = execute(strategy, "fingers>'middle'")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "lte" >> {
        val features = execute(strategy, "fingers<='middle'")
        features must have size(4)
        features must contain("alice", "bill", "bob", "charles")
      }
      "gte" >> {
        val features = execute(strategy, "fingers>='middle'")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "fingers BETWEEN 'pinkie' AND 'thumb'")
        features must have size(3)
        features must contain("bill", "bob", "charles")
      }
    }

    "correctly query on lists of doubles" in {
      "lt" >> {
        val features = execute(strategy, "toes<2.0")
        features must have size(2)
        features must contain("alice", "bill")
      }
      "gt" >> {
        val features = execute(strategy, "toes>2.0")
        features must have size(1)
        features must contain("bob")
      }
      "lte" >> {
        val features = execute(strategy, "toes<=2.0")
        features must have size(3)
        features must contain("alice", "bill", "bob")
      }
      "gte" >> {
        val features = execute(strategy, "toes>=2.0")
        features must have size(2)
        features must contain("bill", "bob")
      }
      "between (inclusive)" >> {
        val features = execute(strategy, "toes BETWEEN 1.5 AND 2.5")
        features must have size(2)
        features must contain("bill", "bob")
      }
    }
  }

  "AttributeIndexLikeStrategy" should {

    val strategy = new AttributeIdxLikeStrategy

    "correctly query on strings" in {
      val features = execute(strategy, "name LIKE 'b%'")
      features must have size(2)
      features must contain("bill", "bob")
    }
  }
}
