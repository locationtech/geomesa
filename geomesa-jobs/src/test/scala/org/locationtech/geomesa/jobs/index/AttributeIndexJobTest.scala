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

package org.locationtech.geomesa.jobs.index

import cascading.tuple.Tuple
import com.twitter.scalding.{Source, Args, Mode}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.AccumuloFeatureWriter.FeatureToWrite
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.index.IndexValueEncoder
import org.locationtech.geomesa.core.iterators.TestData
import org.locationtech.geomesa.core.iterators.TestData._
import org.locationtech.geomesa.feature.{SimpleFeatureDecoder, SimpleFeatureEncoder}
import org.locationtech.geomesa.jobs.index.AttributeIndexJob._
import org.locationtech.geomesa.jobs.scalding.{AccumuloSource, GeoMesaInputOptions, GeoMesaSource, ConnectionParams}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class AttributeIndexJobTest extends Specification {

  val tableName = "AttributeIndexJobTest"

  val params = Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1,zoo2,zoo3",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "auths"             -> "",
    "tableName"         -> tableName,
    "useMock"           -> "true")

  val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

  def test(sft: SimpleFeatureType, feats: Seq[SimpleFeature]) = {
    getFeatureStore(ds, sft, feats) // populate the data

    val jobParams = Map(ATTRIBUTES_TO_INDEX -> List("attr2"),
                        INDEX_COVERAGE -> List("join"),
                        ConnectionParams.FEATURE_IN -> List(sft.getTypeName))
    val scaldingArgs = new Args(ConnectionParams.toInArgs(params) ++ jobParams)

    val input = feats.map(f => new Tuple(new Text(f.getID), f)).toBuffer
    val output = mutable.Buffer.empty[Tuple]
    val buffers: (Source) => Option[mutable.Buffer[Tuple]] = {
      case gs: GeoMesaSource  => Some(input)
      case as: AccumuloSource => Some(output)
    }
    val arguments = Mode.putMode(com.twitter.scalding.Test(buffers), scaldingArgs)

    // run the job
    val job = new AttributeIndexJob(arguments)
    job.run must beTrue

    val descriptor = sft.getDescriptor("attr2")
    val attrList = Seq((descriptor, sft.indexOf(descriptor.getName)))
    val prefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(sft)
    val indexValueEncoder = IndexValueEncoder(sft, ds.getGeomesaVersion(sft))
    val encoder = SimpleFeatureEncoder(sft, ds.getFeatureEncoding(sft))

    val expectedMutations = feats.flatMap { sf =>
      val toWrite = new FeatureToWrite(sf, ds.writeVisibilities, encoder, indexValueEncoder)
      AttributeTable.getAttributeIndexMutations(toWrite, attrList, prefix)
    }

    val jobMutations = output.map(_.getObject(1).asInstanceOf[Mutation])
    jobMutations must containTheSameElementsAs(expectedMutations)
  }

  "AccumuloIndexJob" should {
    "create the correct mutation for a stand-alone feature" in {
      val sft1 = TestData.getFeatureType("1", tableSharing = false)
      val mediumData1: Seq[SimpleFeature] = mediumData.map(createSF(_, sft1))
      test(sft1, mediumData1)
    }

    "create the correct mutation for a shared-table feature" in {
      val sft2 = TestData.getFeatureType("2", tableSharing = true)
      val mediumData2 = mediumData.map(createSF(_, sft2))
      test(sft2, mediumData2)
    }
  }
}
