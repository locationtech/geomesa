package geomesa.core.data

import collection.JavaConversions._
import geomesa.utils.geotools.Conversions._
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.commons.codec.binary.Hex
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{Query, DataUtilities, DataStoreFinder}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


/**
 * The purpose of this test is to ensure that the table version is backwards compatible with
 * older versions (e.g. 0.10.x). The table format should not be changed without some sort of
 * transition map/reduce job to convert table formats.
 */
@RunWith(classOf[JUnitRunner])
class TableVersionTest extends Specification {

  sequential

  val geotimeAttributes = geomesa.core.index.spec

  val baseParams = Map(
    "instanceId" -> "mycloud",
    "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
    "user"       -> "myuser",
    "password"   -> "mypassword",
    "auths"      -> "A,B,C",
    "useMock"    -> "true")

  val geomesaParams = baseParams ++ Map("tableName" -> "geomesaTable")
  val manualParams = baseParams ++ Map("tableName" -> "manualTable")

  val sftName = "regressionTestType"
  val sft = DataUtilities.createType(sftName, s"name:String,$geotimeAttributes")

  def buildManualTable = {
    val instance = new MockInstance(manualParams("instanceId"))
    val connector = instance.getConnector(manualParams("user"), new PasswordToken(manualParams("password").getBytes))
    connector.tableOperations.create(manualParams("tableName"))

    val bw = connector.createBatchWriter(manualParams("tableName"), new BatchWriterConfig)

    // Insert metadata
    val metadataMutation = new Mutation(s"~METADATA_$sftName")
    metadataMutation.put("attributes", "", s"name:String,geomesa_index_geometry:Geometry:srid=4326,geomesa_index_start_time:Date,geomesa_index_end_time:Date")
    metadataMutation.put("bounds", "", "45.0:45.0:49.0:49.0")
    metadataMutation.put("schema", "", s"%~#s%99#r%$sftName#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id")
    bw.addMutation(metadataMutation)

    // Insert features
    getFeatures.zipWithIndex.foreach { case(sf, idx) =>
      val encoded = DataUtilities.encodeFeature(sf)
      val index = new Mutation(rowIds(idx))
      index.put("00".getBytes,sf.getID.getBytes, indexValues(idx))
      bw.addMutation(index)

      val data = new Mutation(rowIds(idx))
      data.put(sf.getID, "SimpleFeatureAttribute", encoded)
      bw.addMutation(data)
    }

    bw.flush
    bw.close

  }

  val rowIds = List(
    "09~regressionTestType~v00~20120102",
    "95~regressionTestType~v00~20120102",
    "53~regressionTestType~v00~20120102",
    "77~regressionTestType~v00~20120102",
    "36~regressionTestType~v00~20120102",
    "91~regressionTestType~v00~20120102")

  val hex = new Hex
  val indexValues = List(
    "000000013000000015000000000140468000000000004046800000000000000001349ccf6e18",
    "000000013100000015000000000140468000000000004046800000000000000001349ccf6e18",
    "000000013200000015000000000140468000000000004046800000000000000001349ccf6e18",
    "000000013300000015000000000140468000000000004046800000000000000001349ccf6e18",
    "000000013400000015000000000140468000000000004046800000000000000001349ccf6e18",
    "000000013500000015000000000140468000000000004046800000000000000001349ccf6e18").map {v =>
    hex.decode(v.getBytes)}

  val builder = new SimpleFeatureBuilder(sft)

  def getFeatures = (0 until 6).map { i =>
    builder.reset
    builder.set("geomesa_index_geometry", WKTUtils.read("POINT(45.0 45.0)"))
    builder.set("geomesa_index_start_time", "2012-01-02T05:06:07.000Z")
    builder.set("geomesa_index_end_time", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    builder.buildFeature(i.toString)
  }

  def buildTableWithDataStore = {
    val ds = DataStoreFinder.getDataStore(geomesaParams).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
    fs.addFeatures(new ListFeatureCollection(sft, getFeatures.toList))
  }

  "Geomesa" should {
    "preserve old table format" in {
      buildTableWithDataStore
      buildManualTable

      // scan the manually build table
      val manualDs = DataStoreFinder.getDataStore(manualParams).asInstanceOf[AccumuloDataStore]
      val manualFs = manualDs.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      val query = new Query(sftName, Filter.INCLUDE)

      // scan the manually build table
      val geomesaDs = DataStoreFinder.getDataStore(manualParams).asInstanceOf[AccumuloDataStore]
      val geomsaFs = geomesaDs.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      manualFs.getFeatures(query).features.zip(geomsaFs.getFeatures(query).features).foreach {case (m, g) =>
        m should equalTo(g)
      }
    }
  }
}
