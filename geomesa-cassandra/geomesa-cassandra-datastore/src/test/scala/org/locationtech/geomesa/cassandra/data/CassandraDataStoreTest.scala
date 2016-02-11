/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.io.File

import com.datastax.driver.core.Cluster
import com.vividsolutions.jts.geom.Coordinate
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{Query, DataStore, DataStoreFinder, DataUtilities}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class CassandraDataStoreTest  {

  @Test
  def testDSAccess(): Unit = {
    val ds = getDataStore
    Assert.assertNotNull(ds)
  }


  @Test
  def testCreateSchema(): Unit = {
    val ds = getDataStore
    Assert.assertNotNull(ds)
    ds.createSchema(SimpleFeatureTypes.createType("test:test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date"))
    Assert.assertTrue("Type name is not in the list of typenames", ds.getTypeNames.contains("test"))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testSchemaWithNoDTG(): Unit = {
    val ds = getDataStore
    ds.createSchema(SimpleFeatureTypes.createType("test:nodtg", "name:String,age:Int,*geom:Point:srid=4326"))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testSchemaWithNonPointGeom(): Unit = {
    val ds = getDataStore
    ds.createSchema(SimpleFeatureTypes.createType("test:nodtg", "name:String,age:Int,*geom:Polygon:srid=4326,dtg:Date"))
  }

  @Test
  def testWrite(): Unit = {
    val fs = initializeDataStore("testwrite")
    Assert.assertEquals("Unexpected count of features", fs.getFeatures.features().toList.length, 2)
  }

  @Test
  def testBboxBetweenQuery(): Unit = {
    val fs = initializeDataStore("testbboxbetweenquery")

    val ff = CommonFactoryFinder.getFilterFactory2
    val filt =
      ff.and(ff.bbox("geom", -76.0, 34.0, -74.0, 36.0, "EPSG:4326"),
        ff.between(
          ff.property("dtg"),
          ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
          ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)))

    Assert.assertEquals("Unexpected count of features", 1, fs.getFeatures(filt).features().toList.length)
  }

  @Test
  def testExtraLargeBboxBetweenQuery(): Unit = {
    val fs = initializeDataStore("testextralargebboxbetweenquery")

    val ff = CommonFactoryFinder.getFilterFactory2
    val filt =
      ff.and(ff.bbox("geom", -200.0, -100.0, 200.0, 100.0, "EPSG:4326"),
        ff.between(
          ff.property("dtg"),
          ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
          ff.literal(new DateTime("2016-01-01T00:15:00.000Z").toDate)))

    Assert.assertEquals("Unexpected count of features", 1, fs.getFeatures(filt).features().toList.length)
  }


  @Test
  def testBboxBetweenAndAttributeQuery(): Unit = {
    import scala.collection.JavaConversions._

    val fs = initializeDataStore("testbboxbetweenandattributequery")
    val ff = CommonFactoryFinder.getFilterFactory2
    val filt =
      ff.and(
        List(
          ff.bbox("geom", -76.0, 34.0, -74.0, 39.0, "EPSG:4326"),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)),
          ff.equals(ff.property("name"), ff.literal("jane"))
        )
      )
    Assert.assertEquals("Unexpected count of features", 1, fs.getFeatures(filt).features().toList.length)
  }

  @Test
  def testPolyWithinAndDtgBetweenQuery(): Unit = {
    val fs = initializeDataStore("testpolywithinanddtgbetween")

    val gf = JTSFactoryFinder.getGeometryFactory
    val buf = gf.createPoint(new Coordinate(new Coordinate(-75.0, 35.0))).buffer(0.01)
    val ff = CommonFactoryFinder.getFilterFactory2
    val filt =
      ff.and(ff.within(ff.property("geom"), ff.literal(buf)),
        ff.between(
          ff.property("dtg"),
          ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
          ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)))

    Assert.assertEquals("Unexpected count of features", 1, fs.getFeatures(filt).features().toList.length)
  }

  @Test
  def testCount(): Unit = {
    val fs = initializeDataStore("testcount")

    val gf = JTSFactoryFinder.getGeometryFactory
    val buf = gf.createPoint(new Coordinate(new Coordinate(-75.0, 35.0))).buffer(0.001)
    val ff = CommonFactoryFinder.getFilterFactory2
    val filt =
      ff.and(ff.within(ff.property("geom"), ff.literal(buf)),
        ff.between(
          ff.property("dtg"),
          ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
          ff.literal(new DateTime("2016-01-02T00:00:00.000Z").toDate)))

    Assert.assertEquals("Count should be 1", 1, fs.getCount(new Query("testcount", filt)))
  }

  def initializeDataStore(tableName: String): SimpleFeatureStore = {

    val ds = getDataStore
    val sft = SimpleFeatureTypes.createType(s"test:$tableName", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date")
    ds.createSchema(sft)

    val gf = JTSFactoryFinder.getGeometryFactory

    val fs = ds.getFeatureSource(s"$tableName").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(
      DataUtilities.collection(Array(
        SimpleFeatureBuilder.build(sft, Array("john", 10, gf.createPoint(new Coordinate(-75.0, 35.0)), new DateTime("2016-01-01T00:00:00.000Z").toDate).asInstanceOf[Array[AnyRef]], "1"),
        SimpleFeatureBuilder.build(sft, Array("jane", 20, gf.createPoint(new Coordinate(-75.0, 38.0)), new DateTime("2016-01-07T00:00:00.000Z").toDate).asInstanceOf[Array[AnyRef]], "2")
      ))
    )
    fs
  }

  def getDataStore: DataStore = {
    import scala.collection.JavaConversions._
    DataStoreFinder.getDataStore(
      Map(
        CassandraDataStoreParams.CONTACT_POINT.getName -> CassandraDataStoreTest.CP,
        CassandraDataStoreParams.KEYSPACE.getName -> "geomesa_cassandra",
        CassandraDataStoreParams.NAMESPACEP.getName -> "http://geomesa.org"
      )
    )
  }

}

object CassandraDataStoreTest {
  val HOST = "127.0.0.1"
  val PORT = 19142
  val CP   = s"$HOST:$PORT"

  @BeforeClass
  def startServer() = {
    val storagedir = File.createTempFile("cassandra","sd")
    storagedir.delete()
    storagedir.mkdir()

    System.setProperty("cassandra.storagedir", storagedir.getPath)

    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-config.yaml", 30000L)
    val cluster = new Cluster.Builder().addContactPoints(HOST).withPort(PORT).build()
    val session = cluster.connect()
    val cqlDataLoader = new CQLDataLoader(session)
    cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))
  }


}
