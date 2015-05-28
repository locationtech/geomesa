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

import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator
import org.locationtech.geomesa.accumulo.util.CloseableIterator
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializers, SerializationType, SimpleFeatureDeserializer}
import org.locationtech.geomesa.security._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.sort.SortBy
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.accumulo.data.INTERNAL_GEOMESA_VERSION


@RunWith(classOf[JUnitRunner])
class QueryPlannerTest extends Specification with Mockito with TestWithDataStore {

  override val spec = "*geom:Geometry,dtg:Date,s:String"
  val schema = ds.getIndexSchemaFmt(sftName)
  val sf = new ScalaSimpleFeature("id", sft)
  sf.setAttributes(Array[AnyRef]("POINT(45 45)", "2014/10/10T00:00:00Z", "string"))

  addFeatures(Seq(sf))

  "adaptStandardIterator" should {
    "return a LazySortedIterator when the query has an order by clause" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(Array(SortBy.NATURAL_ORDER))

      val planner = new QueryPlanner(sft, SerializationType.KRYO, schema, ds, NoOpHints, INTERNAL_GEOMESA_VERSION)
      val result = planner.query(query)

      result must beAnInstanceOf[LazySortedIterator]
    }

    "not return a LazySortedIterator when the query does not have an order by clause" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(null)

      val planner = new QueryPlanner(sft, SerializationType.KRYO, schema, ds, NoOpHints, INTERNAL_GEOMESA_VERSION)
      val result = planner.query(query)

      result must not (beAnInstanceOf[LazySortedIterator])
    }

    "decode and set visibility properly" >> {
      val planner = new QueryPlanner(sft, SerializationType.KRYO, schema, ds, NoOpHints, INTERNAL_GEOMESA_VERSION)
      val query = new Query(sft.getTypeName)

      val visibilities = Array("", "USER", "ADMIN")
      val expectedVis = visibilities.map(vis => if (vis.isEmpty) None else Some(vis))

      val serializer = SimpleFeatureSerializers(sft, SerializationType.KRYO)

      val value = new Value(serializer.serialize(sf))
      val kvs =  visibilities.zipWithIndex.map { case (vis, ndx) =>
        val key = new Key(new Text(ndx.toString), new Text("cf"), new Text("cq"), new Text(vis))
        new SimpleEntry[Key, Value](key, value)
      }

      val expectedResult = kvs.map(planner.defaultKVsToFeatures(query)).map(_.visibility)

      expectedResult must haveSize(kvs.length)
      expectedResult mustEqual expectedVis
    }
  }
}
