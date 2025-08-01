/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.MultiValuedFilter.MatchAction
import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.jdbc.JDBCDataStore
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.Config.WalLogEnabled
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures.{DropAgedOffPartitions, PartitionMaintenance, RollWriteAheadLog}
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.{PartitionTablespacesTable, PrimaryKeyTable, SequenceTable, UserDataTable}
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect, TableConfig, TypeInfo}
import org.locationtech.geomesa.process.transform.ArrowConversionProcess.ArrowVisitor
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import java.io.{ByteArrayInputStream, SequenceInputStream}
import java.sql.Connection
import java.util.concurrent.CopyOnWriteArrayList
import java.util.logging.{Handler, Level, LogRecord}
import java.util.{Collections, Locale}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class PartitionedPostgisDataStoreTest extends Specification with BeforeAfterAll with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  val hours = 1
  val spec =
    "name:List[String],props:String:json=true,age:Int,dtg:Date,*geom:Point:srid=4326;" +
        Seq(
          s"pg.partitions.interval.hours=$hours",
          "pg.partitions.cron.minute=0"/*,
          "pg.partitions.pages-per-range=32",
          "pg.partitions.max=2",
          "pg.partitions.tablespace.wa=partition",
          "pg.partitions.tablespace.wa-partitions=partition",
          "pg.partitions.tablespace.main=partition",*/
        ).mkString(",")

  val schema = "public"

  lazy val sft = SimpleFeatureTypes.createType("test", spec)

  lazy val now = System.currentTimeMillis()

  lazy val features = Seq.tabulate(10) { i =>
    val builder = new SimpleFeatureBuilder(sft)
    builder.set("name", java.util.List.of(s"name$i", s"alt$i"))
    builder.set("age", i)
    builder.set("props", s"""["name$i"]""")
    builder.set("dtg", new java.util.Date(now - ((i + 1) * 20 * 60 * 1000))) // 20 minutes
    builder.set("geom", WKTUtils.read(s"POINT(0 $i)"))
    builder.buildFeature(s"fid$i")
  }

  lazy val params = Map(
    "dbtype" -> PartitionedPostgisDataStoreParams.DbType.sample,
    "host" -> host,
    "port" -> port,
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> Option(container).fold("postgres")(_.password),
    "Batch insert size" -> "10",
    "preparedStatements" -> "true"
  )

  def isTableLoggedQuery(tableName: String, schemaName: String): String =
    s"""
      |SELECT
      |    n.nspname AS schema_name,
      |    c.relname AS table_name,
      |    CASE c.relpersistence
      |        WHEN 'u' THEN 'unlogged'
      |        WHEN 'p' THEN 'permanent'
      |        WHEN 't' THEN 'temporary'
      |        ELSE 'unknown'
      |    END AS table_type
      |FROM
      |    pg_class c
      |JOIN
      |    pg_namespace n ON n.oid = c.relnamespace
      |WHERE
      |    c.relname = '$tableName'
      |    AND n.nspname = '$schemaName';
      |
      |""".stripMargin

  var container: PostgisContainer = _

  lazy val host = Option(container).map(_.getHost).getOrElse("localhost")
  lazy val port = Option(container).map(_.getFirstMappedPort).getOrElse(5432).toString

  lazy val fif = CommonFactoryFinder.getFilterFactory

  override def beforeAll(): Unit = {
    container = new PostgisContainer()
    if (logger.underlying.isTraceEnabled()) {
      container.withLogAllStatements()
    }
    container.start()
  }

  override def afterAll(): Unit = {
    if (container != null) {
      container.close()
    }
  }

  "PartitionedPostgisDataStore" should {

    "fail with a useful error message if type name is too long" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        // This sft name exceeds 31 characters, so it should fail
        val sft = SimpleFeatureTypes.renameSft(this.sft, "abcdefghijklmnopqrstuvwxyzabcde_____")
        ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
        ds.createSchema(sft) must throwAn[java.io.IOException].like {
          case e => e.getCause.getMessage mustEqual "Can't create schema: type name exceeds max supported length of 31 characters"
        }
      } finally {
        ds.dispose()
      }
      ok
    }

    "create logged tables" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        val sft = SimpleFeatureTypes.renameSft(this.sft, "logged_test")

        ds.createSchema(sft)

        val typeInfo: TypeInfo = TypeInfo(this.schema, sft)

        Seq(
          typeInfo.tables.mainPartitions.name.raw,
          typeInfo.tables.writeAheadPartitions.name.raw,
          typeInfo.tables.spillPartitions.name.raw,
          typeInfo.tables.analyzeQueue.name.raw,
          typeInfo.tables.sortQueue.name.raw).forall { tableName =>
          val sql = isTableLoggedQuery(tableName, "public")
          // verify that the table is logged
          WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.createStatement()) { st =>
              WithClose(st.executeQuery(sql)) { rs =>
                rs.next() must beTrue
                logger.debug(s"Table ${rs.getString("table_name")} is ${rs.getString("table_type")}")
                rs.getString("table_type") mustEqual "permanent"
              }
            }
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "create unlogged tables" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        val sft = SimpleFeatureTypes.renameSft(this.sft, "unlogged_test")
        sft.getUserData.put(WalLogEnabled, "false")

        ds.createSchema(sft)

        val typeInfo: TypeInfo = TypeInfo(this.schema, sft)

        Seq(
          typeInfo.tables.mainPartitions.name.raw,
          typeInfo.tables.writeAheadPartitions.name.raw,
          //          typeInfo.tables.writeAhead.name.raw, write ahead table is created with PartitionedPostgisDialect#encodePostCreateTable
          //          which doesnt have access to the user data, should be ok because the write ahead main table doesnt have any data
          typeInfo.tables.spillPartitions.name.raw,
          typeInfo.tables.analyzeQueue.name.raw,
          typeInfo.tables.sortQueue.name.raw).forall { tableName =>
          val sql = isTableLoggedQuery(tableName, "public")
          // verify that the table is unlogged
          WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.createStatement()) { st =>
              WithClose(st.executeQuery(sql)) { rs =>
                rs.next() must beTrue
                logger.debug(s"Table ${rs.getString("table_name")} is ${rs.getString("table_type")}")
                rs.getString("table_type") mustEqual "unlogged"
              }
            }
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "work" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        val sftNames: Seq[String] = Seq("test", "test-abcdefghijklmnopqrstuvwxyz")

        foreach(sftNames) { name =>
          val sft = SimpleFeatureTypes.renameSft(this.sft, name)
          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          ds.createSchema(sft)

          val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
          schema must not(beNull)
          schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
          logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

          // write some data
          WithClose(new DefaultTransaction()) { tx =>
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              features.foreach { feature =>
                FeatureUtils.write(writer, feature, useProvidedFid = true)
              }
            }
            tx.commit()
          }

          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            val result = SelfClosingIterator(reader).toList
            result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid(_, sft)))
          }

          // verify data is being partitioned as expected
          WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
            val typeInfo = TypeInfo(this.schema, sft)
            // initially everything is in the write ahead log
            foreach(Seq(typeInfo.tables.view, typeInfo.tables.writeAhead))(table => count(cx, table) mustEqual 10)
            foreach(Seq(typeInfo.tables.writeAheadPartitions, typeInfo.tables.mainPartitions))(table => count(cx, table) mustEqual 0)
            // manually invoke the scheduled crons so we don't have to wait
            WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
            WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
            // verify that data was sorted into the appropriate tables based on dtg
            count(cx, typeInfo.tables.view) mustEqual 10
            count(cx, typeInfo.tables.writeAhead) mustEqual 0
            count(cx, typeInfo.tables.writeAheadPartitions) must beGreaterThan(0)
            count(cx, typeInfo.tables.mainPartitions) must beGreaterThan(0)
          }

          // ensure we still get same results after running partitioning
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            val result = SelfClosingIterator(reader).toList
            result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid(_, sft)))
          }

          features.foreach { feature =>
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter(s"IN('${feature.getID}')"), Transaction.AUTO_COMMIT)) { writer =>
              writer.hasNext must beTrue
              writer.next()
              writer.remove()
            }
          }

          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            reader.hasNext must beFalse
          }
        }
      } catch {
        case NonFatal(e) => logger.error("", e); ko
      } finally {
        ds.dispose()
      }
    }

    "filter on list elements" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        val sft = SimpleFeatureTypes.renameSft(this.sft, "list-filters")
        ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
        ds.createSchema(sft)

        val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
        schema must not(beNull)
        schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
        logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

        // write some data
        WithClose(new DefaultTransaction()) { tx =>
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
            features.foreach { feature =>
              FeatureUtils.write(writer, feature, useProvidedFid = true)
            }
          }
          tx.commit()
        }

        val filters = Seq(
          fif.equals(fif.property("name"), fif.literal("name0")),
          fif.equal(fif.property("name"), fif.literal("name0"), false, MatchAction.ANY),
          fif.equals(fif.property("name"), fif.literal(Collections.singletonList("name0"))),
          fif.equal(fif.property("name"), fif.literal(Collections.singletonList("name0")), false, MatchAction.ANY),
          fif.equal(fif.property("name"), fif.literal(java.util.List.of("name0", "alt0")), false, MatchAction.ANY),
          fif.equal(fif.property("name"), fif.literal(java.util.List.of("name0", "alt0")), false, MatchAction.ALL),
          ECQL.toFilter("name = 'name0'"),
        )
        foreach(filters) { filter =>
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
            val result = SelfClosingIterator(reader).toList
            result must haveLength(1)
            compFromDb(result.head) mustEqual compWithFid(features.head, sft)
          }
        }

        val orFilters = Seq(
          ECQL.toFilter("name IN('name0','name1')"),
          ECQL.toFilter("name = 'name0' OR name = 'name1'"),
        )
        foreach(orFilters) { filter =>
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
            val result = SelfClosingIterator(reader).toList
            result must haveLength(2)
            result.map(compFromDb) must containTheSameElementsAs(features.take(2).map(compWithFid(_, sft)))
          }
        }

        val nonMatchingFilters = Seq(
          fif.equal(fif.property("name"), fif.literal("name0"), false, MatchAction.ALL),
          fif.equal(fif.property("name"), fif.literal(Collections.singletonList("name0")), false, MatchAction.ALL),
          ECQL.toFilter("name = 'name0' AND name = 'name1'"),
        )
        foreach(nonMatchingFilters) { filter =>
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
            val result = SelfClosingIterator(reader).toList
            result must beEmpty
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "run arrow queries with dictionary encoded list attributes" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        val sft = SimpleFeatureTypes.renameSft(this.sft, "list-arrow")
        ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
        ds.createSchema(sft)

        val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
        schema must not(beNull)
        schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
        logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")
        schema.getDescriptor("name").getUserData.get(AttributeConfigs.UserDataListType) mustEqual "java.lang.String"
        schema.getDescriptor("name").getListType mustEqual classOf[String]

        // write some data
        WithClose(new DefaultTransaction()) { tx =>
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
            features.foreach { feature =>
              FeatureUtils.write(writer, feature, useProvidedFid = true)
            }
          }
          tx.commit()
        }

        val arrowVisitor =
          new ArrowVisitor(schema, SimpleFeatureEncoding.min(includeFids = true).copy(date = Encoding.Max), "18.3.0",
            Seq("name"), Some("dtg"), Some(true), preSorted = false, 100, flattenStruct = false)

        ds.getFeatureSource(sft.getTypeName).getFeatures.accepts(arrowVisitor, null)

        val is = new SequenceInputStream(arrowVisitor.getResult().results.asScala.map(new ByteArrayInputStream(_)).asJavaEnumeration)
        WithClose(SimpleFeatureArrowFileReader.streaming(is)) { reader =>
          WithClose(reader.features())(_.map(compFromDb).toList) mustEqual features.map(compWithFid(_, sft))
        }
      } finally {
        ds.dispose()
      }
    }

    "insert data without requiring JAI on the classpath" in {
      val ds = DataStoreFinder.getDataStore((params ++ Map("Batch insert size" -> "1")).asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        val sft = SimpleFeatureTypes.createType("jai", "name:String,dtg:Date,dtg2:Date,*geom:Point:srid=4326")
        ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
        ds.createSchema(sft)

        WithClose(new DefaultTransaction()) { tx =>
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
            val next = writer.next()
            next.setAttribute(0, "name")
            next.setAttribute(1, "2025-07-01T00:00:00.000Z")
            next.setAttribute(2, "")
            next.setAttribute(3, WKTUtils.read("POINT(0 0)"))
            writer.write() must not (throwA[NoClassDefFoundError])
          }
          tx.commit()
        }
        ok
      } catch {
        case NonFatal(e) => logger.error("", e); ko
      } finally {
        ds.dispose()
      }
    }

    "age-off" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        foreach(Seq("age-off", "ageoff")) { name =>
          val sft = SimpleFeatureTypes.renameSft(this.sft, name)
          sft.getUserData.put("pg.partitions.max", "2")

          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          ds.createSchema(sft)

          val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
          schema must not(beNull)
          schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
          logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

          // write some data
          WithClose(new DefaultTransaction()) { tx =>
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              features.foreach { feature =>
                FeatureUtils.write(writer, feature, useProvidedFid = true)
              }
            }
            tx.commit()
          }

          // verify data is being partitioned as expected
          WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
            val typeInfo = TypeInfo(this.schema, sft)
            // initially everything is in the write ahead log
            foreach(Seq(typeInfo.tables.view, typeInfo.tables.writeAhead))(table => count(cx, table) mustEqual 10)
            foreach(Seq(typeInfo.tables.writeAheadPartitions, typeInfo.tables.mainPartitions)) { table =>
              count(cx, table) mustEqual 0
            }
            // manually invoke the scheduled crons so we don't have to wait
            WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
            WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
            // verify that data was aged off appropriately - exact age-off depends on time test was run
            count(cx, typeInfo.tables.view) must beOneOf(6, 7, 8)
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "re-create functions" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        foreach(Seq("re-create", "recreate")) { name =>
          val sft = SimpleFeatureTypes.renameSft(this.sft, name)

          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          ds.createSchema(sft)

          val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
          schema must not(beNull)
          schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
          logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

          // write some data
          WithClose(new DefaultTransaction()) { tx =>
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              features.foreach { feature =>
                FeatureUtils.write(writer, feature, useProvidedFid = true)
              }
            }
            tx.commit()
          }

          // verify data comes back
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            val result = SelfClosingIterator(reader).toList
            result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid(_, sft)))
          }

          val typeInfo = TypeInfo(this.schema, sft)

          // replace the age-off function so that we can verify it gets updated later
          val oldAgeOff = DropAgedOffPartitions.name(typeInfo)
          val body =
            s"""    BEGIN
               |      SELECT value::int FROM ${typeInfo.schema.quoted}.${UserDataTable.Name.quoted};
               |    END;""".stripMargin
          val sql =
            s"""CREATE OR REPLACE PROCEDURE ${oldAgeOff.quoted}(cur_time timestamp without time zone) LANGUAGE plpgsql AS
               |  $$BODY$$
               |$body
               |  $$BODY$$;
               |""".stripMargin

          WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.prepareStatement(sql))(_.executeUpdate())
            WithClose(cx.prepareStatement(s"SELECT prosrc FROM pg_proc WHERE proname = ${oldAgeOff.asLiteral};")) { st =>
              WithClose(st.executeQuery()) { rs =>
                rs.next() must beTrue
                rs.getString(1).trim mustEqual body.trim
              }
            }
            // now drop the main view
            WithClose(cx.prepareStatement(s"""DROP VIEW "${sft.getTypeName}""""))(_.executeUpdate())
          }

          // verify the feature type no longer returns
          ds.getTypeNames
          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          // re-create the schema, adding some extra user data
          sft.getUserData.put("pg.partitions.max", "2")
          // we have to get a new data store so that it doesn't use the cached entry...
          WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
            ds.createSchema(sft)
            val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
            schema must not(beNull)
            schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)

            // verify data still comes back
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
              val result = SelfClosingIterator(reader).toList
              result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid(_, sft)))
            }
          }

          // verify that the age-off function was re-created
          WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.prepareStatement(s"SELECT prosrc FROM pg_proc WHERE proname = ${oldAgeOff.asLiteral};")) { st =>
              WithClose(st.executeQuery()) { rs =>
                rs.next() must beTrue
                rs.getString(1).trim must not(beEqualTo(body.trim))
              }
            }
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "drop all associated tables on removeSchema" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        foreach(Seq("dropme-test", "dropmetest")) { name =>
          val sft = SimpleFeatureTypes.renameSft(this.sft, name)

          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          ds.createSchema(sft)

          val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
          schema must not(beNull)
          schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
          logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

          // write some data
          WithClose(new DefaultTransaction()) { tx =>
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              features.foreach { feature =>
                FeatureUtils.write(writer, feature, useProvidedFid = true)
              }
            }
            tx.commit()
          }

          // get all the tables associated with the schema
          def getTablesAndIndices: Seq[String] = {
            val tables = ArrayBuffer.empty[String]
            WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.getMetaData.getTables(null, null, "dropme%", null)) { rs =>
                while (rs.next()) {
                  tables += rs.getString(3)
                }
              }
            }
            tables.toSeq
          }

          // get all the procedures and functions associated with the schema
          def getFunctions: Seq[String] = {
            val fns = ArrayBuffer.empty[String]
            WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.getMetaData.getProcedures(null, null, "%dropme%")) { rs =>
                while (rs.next()) {
                  fns += rs.getString(3)
                }
              }
              WithClose(cx.getMetaData.getFunctions(null, null, "%dropme%")) { rs =>
                while (rs.next()) {
                  fns += rs.getString(3)
                }
              }
            }
            fns.toSeq
          }

          // get all the scheduled cron jobs associated with the schema
          def getCrons: Seq[String] = {
            val crons = ArrayBuffer.empty[String]
            WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.prepareStatement("SELECT command from cron.job where command like '%dropme%';")) { st =>
                WithClose(st.executeQuery()) { rs =>
                  while (rs.next()) {
                    crons += rs.getString(1)
                  }
                }
              }
            }
            crons.toSeq
          }

          // get all the user data and other associated metadata
          def getMeta: Seq[String] = {
            val meta = ArrayBuffer.empty[String]
            WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
              Seq(
                (UserDataTable.Name, "type_name", "key"),
                (SequenceTable.Name, "type_name", "value"),
                (PrimaryKeyTable.Name, "table_name", "pk_column"),
                (PartitionTablespacesTable.Name, "type_name", "table_type")
              ).foreach { case (table, where, select) =>
                WithClose(cx.prepareStatement(s"SELECT $select FROM ${table.quoted} WHERE $where like 'dropme%';")) { st =>
                  WithClose(st.executeQuery()) { rs =>
                    while (rs.next()) {
                      meta += rs.getString(1)
                    }
                  }
                }
              }
            }
            meta.toSeq
          }

          // _wa, _wa_partition, _partition, _spill tables + dtg, pk, geom indices for each
          // _analyze_queue, _sort_queue, _wa_000, main view
          getTablesAndIndices must haveLength(20)
          // delete/insert/update/wa triggers
          // analyze_partitions, compact, drop_age_off, merge_wa, part_maintenance, part_wa, roll_wa,
          getFunctions must haveLength(11)
          // log_cleaner, analyze_partitions, roll_wa, partition_maintenance
          getCrons must haveLength(4)
          // 3 tablespaces, 4 user data, 1 seq count, 1 primary key
          getMeta must haveLength(9)

          ds.removeSchema(sft.getTypeName)

          getTablesAndIndices must beEmpty
          getFunctions must beEmpty
          getCrons must beEmpty
          getMeta must beEmpty
        }
      } finally {
        ds.dispose()
      }
    }

    "remove whole-world filters" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        val wholeWorldFilters = {
          import FilterHelper.ff
          import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
          // note: can't use ECQL.toFilter as it tries to load jai and dies
          val geom = ff.property("geom")
          val bbox = ff.bbox(geom, -180, -90, 180, 90, CRS.toSRS(CRS_EPSG_4326))
          val intersects =
            ff.intersects(geom, ff.literal(WKTUtils.read("POLYGON((-190 -100, 190 -100, 190 100, -190 100, -190 -100))")))
          Seq(bbox, intersects)
        }

        foreach(Seq(true, false)) { ignoreFilters =>
          val sft = SimpleFeatureTypes.renameSft(this.sft, s"ignore_$ignoreFilters")
          sft.getUserData.put(PartitionedPostgisDialect.Config.FilterWholeWorld, s"$ignoreFilters")

          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          ds.createSchema(sft)

          val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
          schema must not(beNull)
          schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
          logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

          // write some data
          WithClose(new DefaultTransaction()) { tx =>
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              features.foreach { feature =>
                FeatureUtils.write(writer, feature, useProvidedFid = true)
              }
            }
            tx.commit()
          }

          foreach(wholeWorldFilters) { filter =>
            val Array(left, right) = ds.asInstanceOf[JDBCDataStore].getSQLDialect.splitFilter(filter, schema)
            if (ignoreFilters) {
              left mustEqual Filter.INCLUDE
            } else {
              left mustEqual filter
            }
            right mustEqual Filter.INCLUDE

            // track the messages logged by the JDBC store to verify the filter being run
            val messages = new CopyOnWriteArrayList[(String, String)]()
            val threadId = Thread.currentThread().getId
            val handler = new Handler() {
              override def publish(record: LogRecord): Unit =
                if (record.getThreadID == threadId) { messages.add((record.getSourceMethodName, record.getMessage)) }
              override def flush(): Unit = {}
              override def close(): Unit = {}
            }
            val logger = ds.asInstanceOf[JDBCDataStore].getLogger
            logger.setLevel(Level.FINE)
            logger.addHandler(handler)
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
              val result = SelfClosingIterator(reader).toList
              result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid(_, sft)))
            }

            val selects = messages.asScala.collect { case ("selectSQLPS", v) => v }
            selects must haveLength(1)
            if (ignoreFilters) {
              selects.head.toLowerCase(Locale.US) must not(contain("where"))
            } else {
              selects.head.toLowerCase(Locale.US) must contain("where")
            }
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "default to using prepared statements" in {
      foreach(Seq(params, params + ("preparedStatements" -> "true"), params - "preparedStatements")) { params =>
        val ds = DataStoreFinder.getDataStore(params.asJava)
        ds must not(beNull)
        try {
          ds must beAnInstanceOf[JDBCDataStore]
          ds.asInstanceOf[JDBCDataStore].getSQLDialect must beAnInstanceOf[PartitionedPostgisPsDialect]
        } finally {
          ds.dispose()
        }
      }
      foreach(Seq(params + ("preparedStatements" -> "false"))) { params =>
        val ds = DataStoreFinder.getDataStore(params.asJava)
        ds must not(beNull)
        try {
          ds must beAnInstanceOf[JDBCDataStore]
          ds.asInstanceOf[JDBCDataStore].getSQLDialect must beAnInstanceOf[PartitionedPostgisDialect] // not partitioned
        } finally {
          ds.dispose()
        }
      }
    }

    "set appropriate user data for list and json attributes" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        val sft = SimpleFeatureTypes.renameSft(this.sft, "attrtest")
        ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
        ds.createSchema(sft)

        val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
        schema must not(beNull)
        schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
        logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

        ObjectType.selectType(schema.getDescriptor("name")) mustEqual Seq(ObjectType.LIST, ObjectType.STRING)
        ObjectType.selectType(schema.getDescriptor("props")) mustEqual Seq(ObjectType.STRING, ObjectType.JSON)
        ObjectType.selectType(schema.getDescriptor("dtg")) mustEqual Seq(ObjectType.DATE)
        ObjectType.selectType(schema.getDescriptor("geom")) mustEqual Seq(ObjectType.GEOMETRY, ObjectType.POINT)
      } finally {
        ds.dispose()
      }
    }

    "support query interceptors" in {
      val sft = SimpleFeatureTypes.renameSft(this.sft, "interceptor")
      sft.getUserData.put(SimpleFeatureTypes.Configs.QueryInterceptors, classOf[TestQueryInterceptor].getName)

      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
        ds.createSchema(sft)

        val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
        schema must not(beNull)
        schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
        logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

        val Array(left, right) = ds.asInstanceOf[JDBCDataStore].getSQLDialect.splitFilter(Filter.EXCLUDE, schema)
        left mustEqual Filter.INCLUDE
        right mustEqual Filter.INCLUDE

        // write some data
        WithClose(new DefaultTransaction()) { tx =>
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
            features.foreach { feature =>
              FeatureUtils.write(writer, feature, useProvidedFid = true)
            }
          }
          tx.commit()
        }

        // verify that filter is re-written to be Filter.INCLUDE
        WithClose(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("IN('1')")), Transaction.AUTO_COMMIT)) { reader =>
          val result = SelfClosingIterator(reader).toList
          result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid(_, sft)))
        }
      } finally {
        ds.dispose()
      }
    }

    "support idle_in_transaction_session_timeout" in {
      val sft = SimpleFeatureTypes.renameSft(this.sft, "timeout")

      val ds = DataStoreFinder.getDataStore((params ++ Map("idle_in_transaction_session_timeout" -> "500ms", "fetch size" -> 1)).asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
        ds.createSchema(sft)

        val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
        schema must not(beNull)
        schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
        logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(schema)}")

        // write some data
        WithClose(new DefaultTransaction()) { tx =>
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
            features.foreach { feature =>
              FeatureUtils.write(writer, feature, useProvidedFid = true)
            }
          }
          tx.commit()
        }

        // verify that statements will timeout and return an error
        WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
          reader.hasNext must beTrue
          reader.next must not(beNull)
          Thread.sleep(600)
          reader.hasNext must throwAn[Exception]
        }
      } finally {
        ds.dispose()
      }
    }
  }

  def compFromDb(sf: SimpleFeature): Seq[Any] = {
    Seq(sf.getID) ++ sf.getAttributes.asScala.map {
        // even though Timestamp extends Date, equals comparison doesn't work between the 2
        case t: java.sql.Timestamp => new java.util.Date(t.getTime)
        case a => a
      }
  }

  // note: jdbc data store adds the type name into the fid, so we add it here for the comparison
  def compWithFid(sf: SimpleFeature, sft: SimpleFeatureType): Seq[Any] =
    Seq(s"${sft.getTypeName}.${sf.getID}") ++ sf.getAttributes.asScala

  def count(cx: Connection, table: TableConfig): Int = {
    WithClose(cx.prepareStatement(s"select count(*) from ${table.name.qualified};")) { statement =>
      WithClose(statement.executeQuery()) { rs =>
        rs.next() must beTrue
        rs.getInt(1)
      }
    }
  }
}
