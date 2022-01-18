/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io._
import java.nio.charset.StandardCharsets

import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{DataStoreFinder, DataUtilities, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.HBaseSystemProperties.TableAvailabilityTimeout
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, HBaseCatalogParam}
import org.locationtech.geomesa.hbase.utils.HBaseVersions
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseBackCompatibilityTest extends Specification with LazyLogging  {

  import scala.collection.JavaConverters._

  sequential

  val name = "BackCompatibilityTest"

  val sft = SimpleFeatureTypes.createType(name, "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326")

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"${i % 5}", s"2015-01-01T0$i:01:00.000Z", s"POINT(-12$i 4$i)")
  }

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
    null,
    Array("geom"),
    Array("geom", "name")
  )

  val path = "src/test/resources/data/" // note: if running through intellij, use an absolute path

  lazy val params = Map(ConnectionParam.getName -> MiniCluster.connection, HBaseCatalogParam.getName -> name).asJava

  "HBase data store" should {

    "Support back-compatibility to version 2.0.2" in { runVersionTest("2.0.2") }
    "Support back-compatibility to version 2.3.1" in { runVersionTest("2.3.1") }

    "Write data to disk" in {

      skipped("integration")

      val version = "2.0.2"

      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      try {
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      } finally {
        ds.dispose()
      }

      writeVersion(new File(s"$path/versioned-data-$version.kryo"))

      ok
    }
  }

  def runVersionTest(version: String): MatchResult[_] = {
    restoreVersion(new File(s"$path/versioned-data-$version.kryo"))

    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
    try {
      val schema = ds.getSchema(name)
      schema must not(beNull)

      val fs = ds.getFeatureSource(name)

      // test adding features
      val featureToAdd = ScalaSimpleFeature.create(sft, "10", "name10", "10", "2016-01-01T00:30:00.000Z", "POINT(-110 45)")

      val writer = ds.getFeatureWriterAppend(name, Transaction.AUTO_COMMIT)
      FeatureUtils.write(writer, featureToAdd, useProvidedFid = true)
      writer.close()

      // make sure we can read it back
      foreach(addQueries) { query =>
        val filter = ECQL.toFilter(query)
        foreach(transforms) { transform =>
          doQuery(fs, new Query(name, filter, transform), Seq(featureToAdd))
        }
      }

      // delete it
      var remover = ds.getFeatureWriter(name, ECQL.toFilter("IN ('10')"), Transaction.AUTO_COMMIT)
      remover.hasNext must beTrue
      remover.next
      remover.remove()
      remover.hasNext must beFalse
      remover.close()

      // make sure that it no longer comes back
      foreach(addQueries) { query =>
        val filter = ECQL.toFilter(query)
        foreach(transforms) { transform =>
          doQuery(fs, new Query(name, filter, transform), Seq.empty)
        }
      }

      // test queries
      foreach(queries) { case (q, results) =>
        val filter = ECQL.toFilter(q)
        logger.debug(s"Running query $q")
        foreach(transforms) { transform =>
          doQuery(fs, new Query(name, filter, transform), results.map(features.apply))
        }
        doArrowQuery(fs, new Query(name, filter)) must containTheSameElementsAs(results)
      }

      // delete one of the old features
      remover = ds.getFeatureWriter(name, ECQL.toFilter("IN ('5')"), Transaction.AUTO_COMMIT)
      remover.hasNext must beTrue
      remover.next
      remover.remove()
      remover.hasNext must beFalse
      remover.close()

      // make sure that it no longer comes back
      foreach(queries) { case (q, results) =>
        val filter = ECQL.toFilter(q)
        logger.debug(s"Running query $q")
        doQuery(fs, new Query(name, filter), results.filter(_ != 5).map(features.apply))
      }

    } finally {
      ds.dispose()
    }

    ok
  }

  def doQuery(fs: SimpleFeatureSource, query: Query, expected: Seq[SimpleFeature]): MatchResult[_] = {
    logger.debug(s"Running query ${ECQL.toCQL(query.getFilter)} :: " +
        Option(query.getPropertyNames).map(_.mkString(",")).getOrElse("All"))

    val results = SelfClosingIterator(fs.getFeatures(query).features).toList
    if (logger.underlying.isDebugEnabled()) {
      results.foreach(f => logger.debug(DataUtilities.encodeFeature(f)))
    }

    val transformed = {
      val subtype = DataUtilities.createSubType(sft, query.getPropertyNames)
      // note: we have to copy the SimpleFeatureImpl as its `equals` method checks for the implementing class
      expected.map(e => ScalaSimpleFeature.copy(DataUtilities.reType(subtype, e)))
    }

    results must containTheSameElementsAs(transformed)
  }

  def doArrowQuery(fs: SimpleFeatureSource, query: Query): Seq[Int] = {
    query.getHints.put(QueryHints.ARROW_ENCODE, java.lang.Boolean.TRUE)
    val out = new ByteArrayOutputStream
    val results = SelfClosingIterator(fs.getFeatures(query).features)
    results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
    def in() = new ByteArrayInputStream(out.toByteArray)
    WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
      WithClose(reader.features())(_.map(_.getID.toInt).toList)
    }
  }

  def writeVersion(file: File): Unit = {
    val fs = new FileOutputStream(file)
    WithClose(new Output(fs)) { output =>
      def writeBytes(value: Array[Byte]): Unit = writeBytesSubset(value, 0, value.length)
      def writeBytesSubset(value: Array[Byte], offset: Int, length: Int): Unit = {
        output.writeInt(length)
        output.write(value, offset, length)
      }

      val tables = WithClose(MiniCluster.connection.getAdmin)(_.listTableNames(s"$name.*"))
      output.writeInt(tables.size)
      tables.foreach { name =>
        val table = MiniCluster.connection.getTable(name)
        val descriptor = table.getTableDescriptor
        writeBytes(descriptor.getTableName.getName)
        output.writeInt(descriptor.getColumnFamilies.length)
        descriptor.getColumnFamilies.foreach(d => writeBytes(d.getName))
        output.writeInt(descriptor.getCoprocessors.size())
        descriptor.getCoprocessors.asScala.foreach(c => writeBytes(c.getBytes(StandardCharsets.UTF_8)))
        WithClose(table.getScanner(new Scan())) { scanner =>
          output.writeInt(scanner.iterator.asScala.length)
        }
        WithClose(table.getScanner(new Scan())) { scanner =>
          scanner.iterator.asScala.foreach { result =>
            val cell = result.rawCells()(0)
            writeBytesSubset(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
            writeBytesSubset(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
            writeBytesSubset(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
            writeBytesSubset(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          }
        }
      }

      output.flush()
    }
  }

  def restoreVersion(file: File): Unit = {
    val input = new Input(new FileInputStream(file))
    def readBytes: Array[Byte] = {
      val bytes = Array.ofDim[Byte](input.readInt)
      input.read(bytes)
      bytes
    }
    // reload the tables
    WithClose(MiniCluster.connection.getAdmin) { admin =>
      val numTables = input.readInt
      var i = 0
      while (i < numTables) {
        i += 1
        val name = TableName.valueOf(readBytes)
        val cols = Seq.fill(input.readInt)(readBytes)
        // TODO jar path, etc?
        val coprocessors = Seq.fill(input.readInt)(new String(readBytes, StandardCharsets.UTF_8))
        if (coprocessors.lengthCompare(1) > 0) {
          throw new IllegalStateException(s"Only expecting one coprocessor, got: ${coprocessors.mkString(", ")}")
        }
        val coprocessor = coprocessors.headOption.map(s => s -> None)
        val numMutations = input.readInt
        val mutations = (0 until numMutations).map { _ =>
          val row = readBytes
          val cf = readBytes
          val cq = readBytes
          val value = readBytes
          val mutation = new Put(row)
          mutation.addColumn(cf, cq, value)
          mutation
        }

        if (admin.tableExists(name)) {
          admin.disableTable(name)
          admin.deleteTable(name)
        }
        HBaseVersions.createTableAsync(admin, name, cols, None, None, None, None, coprocessor, Seq.empty)
        HBaseIndexAdapter.waitForTable(admin, name)
        WithClose(MiniCluster.connection.getBufferedMutator(name)) { mutator =>
          mutations.foreach(mutator.mutate)
          mutator.flush()
        }

        if (logger.underlying.isTraceEnabled()) {
          logger.trace(s"restored $name ${admin.tableExists(name)}")
          val scan = MiniCluster.connection.getTable(name).getScanner(new Scan())
          SelfClosingIterator(scan.iterator.asScala, scan.close()).foreach(r => logger.trace(r.toString))
        }
      }
    }
  }
}
