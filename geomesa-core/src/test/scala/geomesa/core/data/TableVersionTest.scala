package geomesa.core.data

import geomesa.core.index.SF_PROPERTY_START_TIME
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.Conversions._
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.codec.binary.Hex
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{DataStoreFinder, DataUtilities, Query}
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._


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

  val geomesaParams = baseParams ++ Map("tableName" -> "geomesaTable", "featureEncoding" -> "text")
  val manualParams = baseParams ++ Map("tableName" -> "manualTable")

  val sftName = "regressionTestType"
  val sft = SimpleFeatureTypes.createType(sftName, s"name:String,$geotimeAttributes")
  sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

  def buildManualTable(params: Map[String, String]) = {
    val instance = new MockInstance(params("instanceId"))
    val connector = instance.getConnector(params("user"), new PasswordToken(params("password").getBytes))
    connector.tableOperations.create(params("tableName"))

    val bw = connector.createBatchWriter(params("tableName"), new BatchWriterConfig)

    // Insert metadata
    val metadataMutation = new Mutation(s"~METADATA_$sftName")
    metadataMutation.put("attributes", "", s"name:String,geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date")
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

  val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

  def getFeatures = (0 until 6).map { i =>
    builder.reset
    builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("dtg_end_time", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }

  def buildTableWithDataStore(params: Map[String, String]) = {
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
    fs.addFeatures(new ListFeatureCollection(sft, getFeatures.toList))
  }

  "Geomesa" should {
    "preserve old table format" in {
      buildTableWithDataStore(geomesaParams)
      buildManualTable(manualParams)

      val manualStore = DataStoreFinder.getDataStore(manualParams).asInstanceOf[AccumuloDataStore]
      val manualSource = manualStore.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      val query = new Query(sftName, Filter.INCLUDE)

      val geomesaStore = DataStoreFinder.getDataStore(geomesaParams).asInstanceOf[AccumuloDataStore]
      val geomesaSource = geomesaStore.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val manualFeatures = manualSource.getFeatures(query).features.toList.sortBy(_.getID.toInt)
      val geomesaFeatures = geomesaSource.getFeatures(query).features.toList.sortBy(_.getID.toInt)

      manualFeatures must forall { m: SimpleFeature => (m must not).beNull }

      manualFeatures must containTheSameElementsAs(geomesaFeatures)
    }

    "revert to text even when told to use avro if the table isn't avro" in {
      // bad because the table is text, not avro
      val badParams = manualParams ++ Map("featureEncoding" -> "avro")

      // scan the manually built table
      val manualStore = DataStoreFinder.getDataStore(badParams).asInstanceOf[AccumuloDataStore]

      // the user provided avro
      manualStore.featureEncoding mustEqual FeatureEncoding.AVRO

      // Ensure that a table with featureEncoder metadata defaults to TextFeatureEncoder
      // and verify with a manual scanner
      manualStore.getFeatureEncoder(sftName) should beAnInstanceOf[TextFeatureEncoder]

      val instance = new MockInstance(badParams("instanceId"))
      val connector = instance.getConnector(badParams("user"), new PasswordToken(badParams("password").getBytes))
      val scanner = connector.createScanner(badParams("tableName"), new Authorizations())
      scanner.iterator.foreach { entry =>
        if (entry.getKey.getColumnFamily == FEATURE_ENCODING_CF)
          entry.getValue.toString mustEqual FeatureEncoding.TEXT.toString
      }
      scanner.close()

      // Here we are creating a schema AFTER a table already exists...this mimics upgrading
      // from 0.10.x to 1.0.0 ...this call to createSchema should insert a row into the table
      // with the proper feature encoding (effectively upgrading to a "1.0.0" table format)

      // calling createSchema is invalid once the schema has been created - but the store will
      // validate itself upon operation
      // manualStore.createSchema(sft)
      manualStore.getFeatureEncoder(sftName) should beAnInstanceOf[TextFeatureEncoder]
      val scanner2 = connector.createScanner(badParams("tableName"), new Authorizations())
      var hasEncodingMeta = false
      scanner2.iterator.foreach { entry =>
        hasEncodingMeta |= entry.getKey.getColumnFamily.equals(FEATURE_ENCODING_CF)
      }
      hasEncodingMeta must beTrue

      // compare again to ensure we get the same implementation type (aka SimpleFeatureImpl for text)
      val manualSource = manualStore.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      val query = new Query(sftName, Filter.INCLUDE)

      val geomesaStore = DataStoreFinder.getDataStore(geomesaParams).asInstanceOf[AccumuloDataStore]
      val geomesaSource = geomesaStore.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val manualFeatures = manualSource.getFeatures(query).features
      val geomesaFeatures = geomesaSource.getFeatures(query).features

      manualFeatures.size mustEqual 6
      geomesaFeatures.size mustEqual 6

      manualFeatures.toList must containTheSameElementsAs(geomesaFeatures.toList)
    }

    "properly encode text after having data written with 1.0.0 api" in {
      val newManualParams = manualParams.updated("tableName", "manual2")
      val newGeomesaParams = geomesaParams.updated("tableName", "geomesa2")

      buildManualTable(newManualParams)
      buildTableWithDataStore(newGeomesaParams)

      // scan the manually built table
      val manualStore = DataStoreFinder.getDataStore(newManualParams).asInstanceOf[AccumuloDataStore]

      // the user provided avro
      manualStore.featureEncoding mustEqual FeatureEncoding.AVRO

      // Ensure that a table with featureEncoder metadata defaults to TextFeatureEncoder
      // and verify with a manual scanner
      manualStore.getFeatureEncoder(sftName) should beAnInstanceOf[TextFeatureEncoder]

      val instance = new MockInstance(newManualParams("instanceId"))
      val connector = instance.getConnector(newManualParams("user"), new PasswordToken(newManualParams("password").getBytes))
      val scanner = connector.createScanner(newManualParams("tableName"), new Authorizations())
      scanner.iterator.foreach { entry =>
        entry.getKey.getColumnFamily should not(equalTo(FEATURE_ENCODING_CF))
      }
      scanner.close

      builder.reset
      builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
      builder.set("dtg", "2012-01-02T05:06:07.000Z")
      builder.set("dtg_end_time", "2012-01-02T05:06:07.000Z")
      builder.set("name","random")
      val sf = builder.buildFeature("bigid")
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

      // compare again to ensure we get the same implementation type (aka SimpleFeatureImpl for text)
      val manualSource = manualStore.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      manualSource.addFeatures(new ListFeatureCollection(sft, List(sf)))

      val geomesaStore = DataStoreFinder.getDataStore(newGeomesaParams).asInstanceOf[AccumuloDataStore]
      val geomesaSource = geomesaStore.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      geomesaSource.addFeatures(new ListFeatureCollection(sft, List(sf)))

      val query = new Query(sftName, Filter.INCLUDE)

      val manualFeatures = manualSource.getFeatures(query).features
      val geomesaFeatures = geomesaSource.getFeatures(query).features

      manualFeatures.size mustEqual 7
      geomesaFeatures.size mustEqual 7

      manualFeatures.zip(geomesaFeatures).foreach {case (m, g) =>
        m mustEqual g
      }

      manualStore.getFeatureEncoder(sftName) should beAnInstanceOf[TextFeatureEncoder]
      val scanner2 = connector.createScanner(newManualParams("tableName"), new Authorizations())
      var hasEncodingMeta = false
      scanner2.iterator.foreach { entry =>
        hasEncodingMeta |= entry.getKey.getColumnFamily.equals(FEATURE_ENCODING_CF)
        if (entry.getKey.getColumnFamily == FEATURE_ENCODING_CF) {
          entry.getValue.toString mustEqual FeatureEncoding.TEXT.toString
        }
      }

      // the store will validate and update itself upon operation
      hasEncodingMeta must beTrue
    }

    "should default to creating new tables in avro" in {
      var newGeomesaParams = geomesaParams.updated("tableName", "geomesa3")
      newGeomesaParams -= "featureEncoding"

      newGeomesaParams.contains("featureEncoding") must beFalse

      buildTableWithDataStore(newGeomesaParams)

      val geomesaStore = DataStoreFinder.getDataStore(newGeomesaParams).asInstanceOf[AccumuloDataStore]

      geomesaStore.featureEncoding mustEqual FeatureEncoding.AVRO
      geomesaStore.getFeatureEncoder(sftName) should beAnInstanceOf[AvroFeatureEncoder]
    }
  }


}
