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

import com.twitter.scalding.{Args, Mode}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.ColumnVisibility
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.index.IndexValueEncoder
import org.locationtech.geomesa.core.iterators.TestData
import org.locationtech.geomesa.core.iterators.TestData._
import org.locationtech.geomesa.feature.SimpleFeatureEncoder
import org.locationtech.geomesa.jobs.index.AttributeIndexJob._
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
    "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "auths"             -> "A,B,C",
    "tableName"         -> tableName,
    "useMock"           -> "true")

  val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

  val mockInstance = new MockInstance("mycloud")
  val c = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))

  val sft1 = TestData.getFeatureType("1", tableSharing = false)
  val sft2 = TestData.getFeatureType("2", tableSharing = true)

  val mediumData1: Seq[SimpleFeature] = mediumData.map(createSF(_, sft1))
  val mediumData2 = mediumData.map(createSF(_, sft2))

  val fs1 = getFeatureStore(ds, sft1, mediumData1)
  val fs2 = getFeatureStore(ds, sft2, mediumData1)

  def test(sft: SimpleFeatureType, feats: Seq[SimpleFeature]) = {
    val jobParams = Map(ATTRIBUTES_TO_INDEX -> List("attr2"), INDEX_COVERAGE -> List("join"))
    val scaldingArgs = new Args(GeoMesaBaseJob.buildBaseArgs(params, sft.getTypeName) ++ jobParams)
    val arguments = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)), scaldingArgs)

    val recScanner1 = ds.createRecordScanner(sft)
    recScanner1.setRanges(Seq(new org.apache.accumulo.core.data.Range()))
    val sft1Records = recScanner1.iterator().toSeq

    val job = new AttributeIndexJob(arguments) {
      val r = new AttributeIndexResources
      override def run = true
    }

    val jobMutations1 = sft1Records.flatMap(e => job.getMutations(e.getValue, job.r))

    val descriptor = sft.getDescriptor("attr2")
    val attrList = Seq((descriptor, sft.indexOf(descriptor.getName)))
    val prefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(sft)
    val indexValueEncoder = IndexValueEncoder(sft, ds.getGeomesaVersion(sft))
    val encoder = SimpleFeatureEncoder(sft, ds.getFeatureEncoding(sft))
    val tableMutations1 = feats.flatMap { sf =>
      AttributeTable.getAttributeIndexMutations(sf, indexValueEncoder, encoder, attrList,
        new ColumnVisibility(ds.writeVisibilities), prefix)
    }
    forall(tableMutations1) { mut => jobMutations1.exists(mut.equals) }
  }

  "AccumuloIndexJob" should {
    "create the correct mutation for a stand-alone feature" in {
      test(sft1, mediumData1)
    }

    "create the correct mutation for a shared-table feature" in {
      test(sft2, mediumData2)
    }
  }
}
