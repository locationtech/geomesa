package org.locationtech.geomesa.jobs.index

import java.util

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.iterators.TestData
import org.locationtech.geomesa.core.iterators.TestData._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
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
    "useMock"           -> "true",
    "featureEncoding"   -> "avro")

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
    val recScanner1 = ds.createRecordScanner(sft)
    recScanner1.setRanges(Seq(new org.apache.accumulo.core.data.Range()))
    val sft1Records = recScanner1.iterator().toSeq

    val r = JobResources(params, sft.getTypeName, List("attr2"))

    val jobMutations1 = sft1Records.flatMap { e =>
      AttributeIndexJob.getAttributeIndexMutation(r, e.getKey, e.getValue)
    }

    val descriptor = sft.getDescriptor("attr2")
    val attrList = Seq((sft.indexOf(descriptor.getName), descriptor))
    val prefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(sft)
    val tableMutations1 = feats.flatMap { sf =>
      AttributeTable.getAttributeIndexMutations(sf, attrList, new ColumnVisibility(ds.writeVisibilities), prefix)
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
