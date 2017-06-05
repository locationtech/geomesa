/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.io.{File, FileInputStream, FileOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility}
import org.apache.hadoop.io.Text
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{DataStoreFinder, DataUtilities, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class BackCompatibilityTest extends Specification with LazyLogging {

  /**
    * Runs version tests against old data. To add more versions, generate a new data file by running
    * 'BackCompatibilityWriter' against the git tag, then add another call to 'testVersion'.
    */

  sequential

  lazy val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))

  val queries = Seq(
    ("INCLUDE", Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
    ("IN ('0', '5', '7')", Seq(0, 5, 7)),
    ("bbox(geom, -130, 45, -120, 50)", Seq(5, 6, 7, 8, 9)),
    ("bbox(geom, -130, 45, -120, 50) AND dtg DURING 2015-01-01T00:00:00.000Z/2015-01-01T07:59:59.999Z", Seq(5, 6, 7)),
    ("name = 'name5'", Seq(5)),
    ("name = 'name5' AND bbox(geom, -130, 45, -120, 50) AND dtg DURING 2015-01-01T00:00:00.000Z/2015-01-01T07:59:59.999Z", Seq(5)),
    ("name = 'name5' AND dtg DURING 2015-01-01T00:00:00.000Z/2015-01-01T07:59:59.999Z", Seq(5)),
    ("name = 'name5' AND bbox(geom, -130, 40, -120, 50)", Seq(5)),
    ("dtg DURING 2015-01-01T00:00:00.000Z/2015-01-01T07:59:59.999Z", Seq(0, 1, 2, 3, 4, 5, 6, 7))
  )

  val addQueries = Seq(
    "IN ('10')",
    "name = 'name10'",
    "bbox(geom, -111, 44, -109, 46)",
    "bbox(geom, -111, 44, -109, 46) AND dtg DURING 2016-01-01T00:00:00.000Z/2016-01-01T01:00:00.000Z"
  )

  val transforms = Seq(
    Array("geom"),
    Array("geom", "name")
  )

  def doQuery(fs: SimpleFeatureSource, query: Query): Seq[Int] = {
    logger.debug(s"Running query ${ECQL.toCQL(query.getFilter)} :: " +
        Option(query.getPropertyNames).map(_.mkString(",")).getOrElse("All"))
    SelfClosingIterator(fs.getFeatures(query).features).toList.map { f =>
      logger.debug(DataUtilities.encodeFeature(f))
      f.getID.toInt
    }
  }

  def runVersionTest(tables: Seq[TableMutations]): MatchResult[Any] = {
    import scala.collection.JavaConversions._

    // since we re-use the same sft and tables, the converter cache can get messed up
    // note that the only problem is the attribute table name change between 1.2.2 and 1.2.3, which gets cached
    // other changes are captured in the index versions, and the cache handles them appropriately
    GeoMesaFeatureWriter.expireConverterCache()

    // reload the tables
    tables.foreach { case TableMutations(table, mutations) =>
      if (connector.tableOperations.exists(table)) {
        connector.tableOperations.delete(table)
      }
      connector.tableOperations.create(table)
      val bw = connector.createBatchWriter(table, new BatchWriterConfig)
      bw.addMutations(mutations)
      bw.flush()
      bw.close()
    }

    // get the data store
    val sftName = tables.map(_.table).minBy(_.length)
    val ds = DataStoreFinder.getDataStore(Map(
      "connector" -> connector,
      "caching"   -> false,
      "tableName" -> sftName
    ))
    val fs = ds.getFeatureSource(sftName)

    // test adding features
    val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)
    val feature = writer.next()
    feature.getIdentifier.asInstanceOf[FeatureIdImpl].setID("10")
    feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    feature.setAttribute(0, "name10")
    feature.setAttribute(1, "2016-01-01T00:30:00.000Z")
    feature.setAttribute(2, "POINT(-110 45)")
    if (feature.getFeatureType.getAttributeCount > 3) {
      feature.setAttribute(3, "MULTIPOLYGON(((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))")
    }
    writer.write()
    writer.close()

    // make sure we can read it back
    forall(addQueries) { query =>
      val filter = ECQL.toFilter(query)
      doQuery(fs, new Query(sftName, filter)) mustEqual Seq(10)
      forall(transforms) { transform =>
        doQuery(fs, new Query(sftName, filter, transform)) mustEqual Seq(10)
      }
    }

    // delete it
    var remover = ds.getFeatureWriter(sftName, ECQL.toFilter("IN ('10')"), Transaction.AUTO_COMMIT)
    remover.hasNext must beTrue
    remover.next
    remover.remove()
    remover.hasNext must beFalse
    remover.close()

    // make sure that it no longer comes back
    forall(addQueries) { query =>
      val filter = ECQL.toFilter(query)
      doQuery(fs, new Query(sftName, filter)) must beEmpty
      forall(transforms) { transform =>
        doQuery(fs, new Query(sftName, filter, transform)) must beEmpty
      }
    }

    // test queries
    forall(queries) { case (q, results) =>
      val filter = ECQL.toFilter(q)
      logger.debug(s"Running query $q")
      doQuery(fs, new Query(sftName, filter)) must containTheSameElementsAs(results)
      forall(transforms) { transform =>
        doQuery(fs, new Query(sftName, filter, transform)) must containTheSameElementsAs(results)
      }
    }

    // delete one of the old features
    remover = ds.getFeatureWriter(sftName, ECQL.toFilter("IN ('5')"), Transaction.AUTO_COMMIT)
    remover.hasNext must beTrue
    remover.next
    remover.remove()
    remover.hasNext must beFalse
    remover.close()

    // make sure that it no longer comes back
    forall(queries) { case (q, results) =>
      val filter = ECQL.toFilter(q)
      logger.debug(s"Running query $q")
      doQuery(fs, new Query(sftName, filter)) must containTheSameElementsAs(results.filter(_ != 5))
    }

    ds.dispose()
    ok
  }

  def readVersion(file: File): Seq[TableMutations] = {
    val input = new Input(new FileInputStream(file))
    def readBytes: Array[Byte] = {
      val bytes = Array.ofDim[Byte](input.readInt)
      input.read(bytes)
      bytes
    }
    val numTables = input.readInt
    (0 until numTables).map { _ =>
      val tableName = input.readString
      val numMutations = input.readInt
      val mutations = (0 until numMutations).map { _ =>
        val row = readBytes
        val cf = readBytes
        val cq = readBytes
        val vis = new ColumnVisibility(readBytes)
        val timestamp = input.readLong
        val value = readBytes
        val mutation = new Mutation(row)
        mutation.put(cf, cq, vis, timestamp, value)
        mutation
      }
      TableMutations(tableName, mutations)
    }
  }

  def testVersion(version: String): MatchResult[Any] = {
    val file = new File(s"src/test/resources/data/versioned-data-$version.kryo")
    val data = readVersion(file)
    logger.info(s"Running back compatible test on version $version")
    runVersionTest(data)
  }

  "GeoMesa" should {
    "support backward compatibility to 1.2.0"   >> { testVersion("1.2.0") }
    "support backward compatibility to 1.2.1"   >> { testVersion("1.2.1") }
    "support backward compatibility to 1.2.2"   >> { testVersion("1.2.2") }
    "support backward compatibility to 1.2.3"   >> { testVersion("1.2.3") }
    "support backward compatibility to 1.2.4"   >> { testVersion("1.2.4") }
    // note: data on disk is the same in 1.2.5 and 1.2.6
    "support backward compatibility to 1.2.6"   >> { testVersion("1.2.6") }
    "support backward compatibility to 1.2.7.3" >> { testVersion("1.2.7.3") }
    "support backward compatibility to 1.3.1"   >> { testVersion("1.3.1") }
  }

  case class TableMutations(table: String, mutations: Seq[Mutation])
}

@RunWith(classOf[JUnitRunner])
class BackCompatibilityWriter extends TestWithDataStore {

  override val spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326,multi:MultiPolygon:srid=4326"

  val version = "REPLACEME"

  "AccumuloDataStore" should {
    "write data" in {

      skipped("integration")

      addFeatures((0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, s"2015-01-01T0$i:01:00.000Z")
        sf.setAttribute(2, s"POINT(-12$i 4$i)")
        sf.setAttribute(3, s"MULTIPOLYGON(((4$i 40, 20 45, 45 30, 4$i 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))")
        sf
      })

      val dataFile = new File(s"src/test/resources/data/versioned-data-$version.kryo")
      val fs = new FileOutputStream(dataFile)
      val output = new Output(fs)

      def writeText(text: Text): Unit = {
        output.writeInt(text.getLength)
        output.write(text.getBytes, 0, text.getLength)
      }

      val tables = connector.tableOperations().list().filter(_.startsWith(sftName))
      output.writeInt(tables.size)
      tables.foreach { table =>
        output.writeAscii(table)
        output.writeInt(connector.createScanner(table, new Authorizations()).size)
        connector.createScanner(table, new Authorizations()).foreach { entry =>
          val key = entry.getKey
          Seq(key.getRow, key.getColumnFamily, key.getColumnQualifier, key.getColumnVisibility).foreach(writeText)
          output.writeLong(key.getTimestamp)
          val value = entry.getValue.get
          output.writeInt(value.length)
          output.write(value)
        }
      }

      output.flush()
      output.close()
      ok
    }
  }
}
