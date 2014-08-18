package org.locationtech.geomesa.core.index

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.IndexQueryPlanner._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.iterators.{FEATURE_ENCODING, _}
import org.locationtech.geomesa.core.util.CloseableIterator._
import org.locationtech.geomesa.core.util.{BatchMultiScanner, CloseableIterator, SelfClosingBatchScanner, SelfClosingIterator}
import org.locationtech.geomesa.utils.geohash.GeohashUtils._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial._

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DeciderTest extends Specification {


  val sft = SimpleFeatureTypes.createType("feature", "id:Integer:index=false,*geom:Point:srid=4326:index=true,dtg:Date,attr1:String:index=false,attr2:String:index=true")
  sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

  def getStrategy(filter: Filter): Strategy = {
    val query = new Query(sft.getTypeName)
    query.setFilter(filter)
    Decider.chooseNewStrategy(sft, query)
  }

  "Spatio-temporal filters" should {
    "get the stidx strategy" in {
      val fs = "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
      val filter = ECQL.toFilter(fs)

      val strategy = getStrategy(filter)

      strategy.isInstanceOf[StIdxStrategy] must beTrue
    }
  }

  "Attribute filters" should {
    "get the attribute equals strategy" in {
      val fs = "attr2 = val56"
      val filter = ECQL.toFilter(fs)

      val strategy = getStrategy(filter)

      strategy.isInstanceOf[AttributeEqualsIdxStrategy] must beTrue
    }
  }

  "Attribute filters" should {
    "get the attribute equals strategy" in {
      val fs = "attr1 = val56"
      val filter = ECQL.toFilter(fs)

      val strategy = getStrategy(filter)

      strategy.isInstanceOf[StIdxStrategy] must beTrue
    }
  }

  "Attribute filters" should {
    "get the attribute likes strategy" in {
      val fs = "attr2 ILIKE '2nd1%'"
      val filter = ECQL.toFilter(fs)

      val strategy = getStrategy(filter)

      strategy.isInstanceOf[AttributeLikeIdxStrategy] must beTrue
    }
  }

  "Attribute filters" should {
    "get the stidx strategy if attribute non-indexed" in {
      val fs = "attr1 ILIKE '2nd1%'"
      val filter = ECQL.toFilter(fs)

      val strategy = getStrategy(filter)

      strategy.isInstanceOf[StIdxStrategy] must beTrue
    }
  }



  "Id filters" should {
    "get the attribute equals strategy" in {
      val fs = "IN ('val56')"
      val filter = ECQL.toFilter(fs)

      val strategy = getStrategy(filter)

      strategy.isInstanceOf[RecordIdxStrategy] must beTrue
    }
  }

  "Id and Spatio-temporal filters" should {
    "get the records strategy" in {
      val fs = "IN ('val56') AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
      val filter = ECQL.toFilter(fs)

      val strategy = getStrategy(filter)

      strategy.isInstanceOf[RecordIdxStrategy] must beTrue
    }.pendingUntilFixed
  }



}
