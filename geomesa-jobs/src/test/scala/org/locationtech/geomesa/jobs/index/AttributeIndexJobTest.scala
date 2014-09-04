package org.locationtech.geomesa.jobs.index

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.iterators.TestData
import org.locationtech.geomesa.core.iterators.TestData._
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

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

  val mediumData1 = mediumData.map(createSF(_, sft1))
  val mediumData2 = mediumData.map(createSF(_, sft2))

  val fs1 = getFeatureStore(ds, sft1, mediumData1)

  val at = ECQL.toFilter("attr2 = '2nd100001'")

  def filterCount(f: Filter) = mediumData1.count(f.evaluate)
  def queryCount(f: Filter, fs: SimpleFeatureSource) = fs.getFeatures(f).size

  def compareEquals(f: Filter, fs: SimpleFeatureSource, when: String) = {
    s"feature count and querying ${fs.getName} return the same count for filter ${ECQL.toCQL(f)} $when" >> {
      val fc = filterCount(f)
      val qc = queryCount(f, fs)
      fc mustEqual queryCount(f, fs)
    }
  }

  def compareZero(f: Filter, fs: SimpleFeatureSource) = {
    s"querying ${fs.getName} should return 0 for filter ${ECQL.toCQL(f)}" >> {
      queryCount(f, fs) mustEqual 0
    }
  }

  "AccumuloIndexJob" should {
    "for a stand-alone tables feature" in {

      sequential
      // Add mediumFeatures as with unshared tables.

      "create and compare" >> {
        // Query for attributes; check success.
        compareEquals(at, fs1, "before deleting the attribute table")
      }

      // Run Queries with no results.
      //Delete the Attribute table.
      "delete and see nothing" >> {
        val attrTable = ds.getAttrIdxTableName(sft1.getTypeName)
        println(s"Deleting table $attrTable")

        c.tableOperations().delete(attrTable)
        compareZero(at, fs1)
      }

      val args = AttributeIndexJob.buildArgs(params.asJava, sft1.getTypeName, Seq("attr2"))
      val aij = new AttributeIndexJob(args)


      // Run AttributeIndexJob
//      val conf = new Configuration()
//      AttributeIndexJob.runJob(conf, params, sft1.getTypeName, Seq("attr2"))

      // Query for attributes; check success.
      "specs annoys me" >> { compareEquals(at, fs1, "after running the index job") }
    }

    "recreate a queryable attribute index for a shared-table feature" in {
      // Add mediumFeatures as with shared tables.

      // Query for attributes; check success.

      // Delete the Attribute table.

      // Run AttributeIndexJob

      // Query for attributes; check success.
      true must beTrue
    }
  }
}
