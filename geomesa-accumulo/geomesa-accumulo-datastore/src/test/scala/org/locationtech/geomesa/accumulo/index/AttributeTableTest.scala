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

package org.locationtech.geomesa.accumulo.index

import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToWrite
import org.locationtech.geomesa.accumulo.data.tables.{AttributeIndexRow, AttributeTable}
import org.locationtech.geomesa.accumulo.data.{DEFAULT_ENCODING, INTERNAL_GEOMESA_VERSION}
import org.locationtech.geomesa.features.{SimpleFeatureSerializers, SimpleFeatureSerializer}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Success

@RunWith(classOf[JUnitRunner])
class AttributeTableTest extends Specification {

  val sftName = "mutableType"
  val spec = s"name:String:$OPT_INDEX=true,age:Integer:$OPT_INDEX=true,*geom:Geometry:srid=4326,dtg:Date:$OPT_INDEX=true"
  val sft = SimpleFeatureTypes.createType(sftName, spec)
  sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

    "AttributeTable" should {

      "encode mutations for attribute index" in {
        val descriptors = sft.getAttributeDescriptors.zipWithIndex

        val feature = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "id1")
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        feature.setDefaultGeometry(geom)
        feature.setAttribute("name","fred")
        feature.setAttribute("age",50.asInstanceOf[Any])
        feature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

        val indexValueEncoder = IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION)
        val featureEncoder = SimpleFeatureSerializers(sft, DEFAULT_ENCODING)

        val toWrite = new FeatureToWrite(feature, "", featureEncoder, indexValueEncoder)
        val mutations = AttributeTable.getAttributeIndexMutations(toWrite, descriptors, "")
        mutations.size mustEqual descriptors.length - 1 // for null date
        mutations.map(_.getUpdates.size()) must contain(beEqualTo(1)).foreach
        mutations.map(_.getUpdates.get(0).isDeleted) must contain(beEqualTo(false)).foreach
      }

      "encode mutations for delete attribute index" in {
        val descriptors = sft.getAttributeDescriptors.zipWithIndex

        val feature = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "id1")
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        feature.setDefaultGeometry(geom)
        feature.setAttribute("name","fred")
        feature.setAttribute("age",50.asInstanceOf[Any])
        feature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

        val toWrite = new FeatureToWrite(feature, "", null, null)
        val mutations = AttributeTable.getAttributeIndexMutations(toWrite, descriptors, "", true)
        mutations.size mustEqual descriptors.length - 1 // for null date
        mutations.map(_.getUpdates.size()) must contain(beEqualTo(1)).foreach
        mutations.map(_.getUpdates.get(0).isDeleted) must contain(beEqualTo(true)).foreach
      }

      "decode attribute index rows" in {
        val row = AttributeTable.getAttributeIndexRows("prefix", sft.getDescriptor("age"), 23).head
        val decoded = AttributeTable.decodeAttributeIndexRow("prefix", sft.getDescriptor("age"), row)
        decoded mustEqual(Success(AttributeIndexRow("age", 23)))
      }
    }

}
