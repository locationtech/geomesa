/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.iterators

import collection.JavaConversions._
import collection.JavaConverters._
import com.vividsolutions.jts.geom.{Polygon, Geometry}
import geomesa.core.data.{SimpleFeatureEncoderFactory, SimpleFeatureEncoder}
import geomesa.core.index._
import geomesa.utils.text.WKTUtils
import java.util
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.{IteratorSetting, Connector, BatchWriterConfig, BatchScanner}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.data._
import org.geotools.data.DataUtilities
import org.joda.time.{Interval, DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.util.{Try, Random}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder

@RunWith(classOf[JUnitRunner])
class IndexIteratorTest extends SpatioTemporalIntersectingIteratorTest {

  override def runMockAccumuloTest(label: String,
                          entries: List[TestData.Entry] = TestData.fullData,
                          ecqlFilter: Option[String] = None,
                          numExpectedDataIn: Int = 113,
                          dtFilter: Interval = null,
                          overrideGeometry: Boolean = false,
                          doPrint: Boolean = true): Int = {

    val featureEncoder = SimpleFeatureEncoderFactory.defaultEncoder

    // create the schema, and require de-duplication
    val schema = SpatioTemporalIndexSchema(TestData.schemaEncoding, TestData.featureType, featureEncoder)

    // create the query polygon
    val polygon: Polygon = overrideGeometry match {
      case true => SpatioTemporalIndexSchema.everywhere
      case false => WKTUtils.read(TestData.wktQuery).asInstanceOf[Polygon]
    }

    // create the batch scanner
    val c = TestData.setupMockAccumuloTable(entries, numExpectedDataIn)
    val bs = c.createBatchScanner(TEST_TABLE, TEST_AUTHORIZATIONS, 5)

    val transform: Option[String] = Some("geomesa_index_geometry=geomesa_index_geometry ; " +
      "geomesa_index_start_time=geomesa_index_start_time ;" +
       "geomesa_index_end_time=geomesa_index_end_time ")
    val transformSchema = Some(indexSFT)
    // fetch results from the schema!
    val itr = schema.query(bs, polygon, dtFilter, UnitTestEntryType.getTypeSpec, ecqlFilter, transform, transformSchema)
    //val itr = schema.query(bs, polygon, dtFilter, UnitTestEntryType.getTypeSpec, ecqlFilter)
    // print out the hits
    val retval = if (doPrint) {
      val results: List[Value] = itr.toList
      results.map(value => {
        val simpleFeature = featureEncoder.decode(TestData.featureType, value)
        val attrs = simpleFeature.getAttributes.map(attr => if (attr == null) "" else attr.toString).mkString("|")
        println("[II." + label + "] query-hit:  " + simpleFeature.getID + "=" + attrs)
      })
      results.size
    } else itr.size

    // close the scanner
    bs.close()

    retval
  }
}
