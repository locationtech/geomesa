/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import com.datastax.driver.core.{Cluster, SocketOptions}
import com.vividsolutions.jts.geom.Coordinate
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CassandraDataStoreTest extends Specification {

  sequential

  step {
    CassandraDataStoreTest.startServer()
  }

  "CassandraDataStore" should {

    "allow access" >> {
      val ds = getDataStore
      ds must not(beNull)
      ds.dispose()
      ok
    }

    "create a schema" >> {
      val ds = getDataStore
      ds must not(beNull)
      ds.createSchema(SimpleFeatureTypes.createType("test:test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date"))
      ds.getTypeNames.toSeq must contain("test")
      ds.dispose()
      ok
    }

    "list schemas with with new datastore" >> {
      val ds = getDataStore
      ds must not(beNull)
      ds.createSchema(SimpleFeatureTypes.createType("test:test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date"))
      ds.dispose()
      val ds2 = getDataStore
      ds2.getTypeNames.toSeq must contain("test")
      ds2.dispose()
      ok
    }

    "parse simpleType to Cassandra Types" >> {
      val simpleFeatureType = SimpleFeatureTypes.createType("test:test",
        "string:String,int:Int,float:Float,double:Double,long:Long,boolean:Boolean,*geom:Point:srid=4326,dtg:Date")
      val types = simpleFeatureType.getTypes.asScala.map(_.getBinding)
      types foreach { t =>
        CassandraDataStore.typeMap.get(t) shouldNotEqual null
      }
      ok
    }

    "fail if no dtg in schema" >> {
      val ds = getDataStore
      val sft = SimpleFeatureTypes.createType("test:nodtg", "name:String,age:Int,*geom:Point:srid=4326")
      ds.createSchema(sft) must throwA[IllegalArgumentException]
      ds.dispose()
      ok
    }

    "fail if non-point geom in schema" >> {
      val ds = getDataStore
      val sft = SimpleFeatureTypes.createType("test:nodtg", "name:String,age:Int,*geom:Polygon:srid=4326,dtg:Date")
      ds.createSchema(sft) must throwA[IllegalArgumentException]
      ds.dispose()
      ok
    }

    "write features" >> {
      val (ds, fs) = initializeDataStore("testwrite")
      val features = fs.getFeatures().features()
      features.toList must haveLength(2)
      features.close()
      ds.dispose()
      ok
    }

    "run bbox between queries" >> {
      val (ds, fs) = initializeDataStore("testbboxbetweenquery")

      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(ff.bbox("geom", -76.0, 34.0, -74.0, 36.0, "EPSG:4326"),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)))

      val features = fs.getFeatures(filt).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }

    "run extra-large bbox between queries" >> {
      skipped("intermittent failure")
      val (ds, fs) = initializeDataStore("testextralargebboxbetweenquery")

      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(ff.bbox("geom", -200.0, -100.0, 200.0, 100.0, "EPSG:4326"),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-01T00:15:00.000Z").toDate)))

      val features = fs.getFeatures(filt).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }

    "run bbox between and attribute queries" >> {
      import scala.collection.JavaConversions._

      val (ds, fs) = initializeDataStore("testbboxbetweenandattributequery")
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
      val features = fs.getFeatures(filt).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }

    "run poly within and date between queries" >> {
      val (ds, fs) = initializeDataStore("testpolywithinanddtgbetween")

      val gf = JTSFactoryFinder.getGeometryFactory
      val buf = gf.createPoint(new Coordinate(new Coordinate(-75.0, 35.0))).buffer(0.01)
      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(ff.within(ff.property("geom"), ff.literal(buf)),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)))

      val features = fs.getFeatures(filt).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }

    "return correct counts" >> {
      val (ds, fs) = initializeDataStore("testcount")

      val gf = JTSFactoryFinder.getGeometryFactory
      val buf = gf.createPoint(new Coordinate(new Coordinate(-75.0, 35.0))).buffer(0.001)
      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(ff.within(ff.property("geom"), ff.literal(buf)),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-02T00:00:00.000Z").toDate)))

      fs.getCount(new Query("testcount", filt)) mustEqual 1
      ds.dispose()
      ok
    }

    "preserve column ordering" >> {
      val (ds, fs) = initializeDataStore("testcolumnordering")
      val sft = ds.getSchema("testcolumnordering")
      sft.getAttributeDescriptors.head.getLocalName mustEqual "name"
      ds.dispose()
      ok
    }

    "find points around the world" >> {
      val (ds, fs) = initializeDataStore("testfindingpoints")
      val gf = JTSFactoryFinder.getGeometryFactory

      val testCoords = List(
        ("11", 40, 40, 2014),
        ("12", 40, -40, 2015),
        ("13", -40, 40, 2016),
        ("14", -40, -40, 2014),
        ("15", -179, -89, 2015),
        ("16", -179, 89, 2016),
        ("17", 179, -89, 2014),
        ("18", 179, 89, 2015),
        ("19", 0, 0, 2016),
        ("20", 2, 2, 2014),
        ("21", 179, 1, 2015),
        ("22", 1, 89, 2016)
      )

      val testFeatures = testCoords
        .map({ case (id, x, y, year) =>
          SimpleFeatureBuilder.build(fs.getSchema, Array(id, 30, gf.createPoint(new Coordinate(x, y)), new DateTime(s"$year-01-07T00:00:00.000Z").toDate).asInstanceOf[Array[AnyRef]], id)
        })

      fs.addFeatures(DataUtilities.collection(testFeatures))

      for ((id, x, y, year) <- testCoords) {
        val filt = ECQL.toFilter(s"bbox(geom, ${x - 1}, ${y - 1}, ${x + 1}, ${1 + y}, 'EPSG:4326') and dtg between $year-01-01T00:00:00.000Z and $year-01-08T00:00:00.000Z")
        val features = fs.getFeatures(filt).features().toList
        features must haveLength(1)
        val feature = features.head
        feature.getAttribute("name").toString mustEqual id
      }
      ok
    }
  }


  def initializeDataStore(tableName: String): (DataStore, SimpleFeatureStore) = {

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
    (ds, fs)
  }

  def getDataStore: DataStore = {
    DataStoreFinder.getDataStore(
      Map(
        CassandraDataStoreParams.CONTACT_POINT.getName -> CassandraDataStoreTest.CP,
        CassandraDataStoreParams.KEYSPACE.getName -> "geomesa_cassandra",
        CassandraDataStoreParams.NAMESPACE.getName -> "http://geomesa.org",
        CassandraDataStoreParams.CATALOG.getName -> "geomesa_cassandra"
      )
    )
  }

}

object CassandraDataStoreTest {
  def host = EmbeddedCassandraServerHelper.getHost
  def port = EmbeddedCassandraServerHelper.getNativeTransportPort
  def CP   = s"$host:$port"

  private val started = new AtomicBoolean(false)

  def startServer() = {
    if (started.compareAndSet(false, true)) {
      val storagedir = File.createTempFile("cassandra","sd")
      storagedir.delete()
      storagedir.mkdir()

      System.setProperty("cassandra.storagedir", storagedir.getPath)

      EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-config.yaml", 1200000L)

      var readTimeout: Int = SystemProperty("cassandraReadTimeout", "12000").get.toInt

      if(readTimeout < 0) readTimeout = 12000
      val cluster = new Cluster.Builder().addContactPoints(host).withPort(port)
        .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeout)).build().init()
      val session = cluster.connect()
      val cqlDataLoader = new CQLDataLoader(session)
      cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))
    }
  }
}
