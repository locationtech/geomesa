/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.jdbc.JDBCDataStore
import org.junit.runner.RunWith
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures.{PartitionMaintenance, RollWriteAheadLog}
import org.locationtech.geomesa.gt.partition.postgis.dialect.{TableConfig, TypeInfo}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.images.builder.ImageFromDockerfile

import java.sql.Connection
import java.util.Collections
import scala.util.Try
import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class PartitionedPostgisDataStoreIT extends Specification with LazyLogging {

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

  lazy val sft = SimpleFeatureTypes.createType(s"test", spec)

  lazy val now = System.currentTimeMillis()

  lazy val features = Seq.tabulate(10) { i =>
    val builder = new SimpleFeatureBuilder(sft)
    builder.set("name", Collections.singletonList(s"name$i"))
    builder.set("age", i)
    builder.set("props", s"""["name$i"]""")
    builder.set("dtg", new java.util.Date(now - ((i + 1) * 20 * 60 * 1000))) // 20 minutes
    builder.set("geom", WKTUtils.read(s"POINT(0 $i)"))
    builder.buildFeature(s"fid$i")
  }

  lazy val params = Map(
    "dbtype" -> PartitionedPostgisDataStoreParams.DbType.sample,
    "host" -> container.getHost,
    "port" -> container.getFirstMappedPort.toString,
    "schema" -> schema,
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> "postgres",
    "Batch insert size" -> "10",
    "Commit size" -> "20",
    "preparedStatements" -> "true"
  )

  var container: GenericContainer[_] = _

  step {
    val image =
      new ImageFromDockerfile("testcontainers/postgis_cron", false)
          .withFileFromClasspath(".", "testcontainers")
    container = new GenericContainer(image)
    container.addEnv("POSTGRES_HOST_AUTH_METHOD", "trust")
    container.addExposedPort(5432)
    container.start()
    container.followOutput(new Slf4jLogConsumer(logger.underlying))
  }

  "PartitionedPostgisDataStore" should {
    "work" in {
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)

      try {
        ds must beAnInstanceOf[JDBCDataStore]

        ds.getTypeNames must beEmpty
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

        def compFromDb(sf: SimpleFeature): Seq[Any] = Seq(sf.getID) ++ sf.getAttributes.asScala.map {
          // even though Timestamp extends Date, equals comparison doesn't work between the 2
          case t: java.sql.Timestamp => new java.util.Date(t.getTime)
          case a => a
        }
        // note: jdbc data store adds the type name into the fid, so we add it here for the comparison
        def compWithFid(sf: SimpleFeature): Seq[Any] = Seq(s"${sft.getTypeName}.${sf.getID}") ++ sf.getAttributes.asScala

        WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
          val result = SelfClosingIterator(reader).toList
          result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid))
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
          result.map(compFromDb) must containTheSameElementsAs(features.map(compWithFid))
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

        ds.removeSchema(sft.getTypeName)
      } catch {
        case NonFatal(e) => logger.error("", e); ko
      } finally {
        ds.dispose()
      }
      ok
    }
  }

  step {
    if (container != null) {
      container.stop()
    }
  }

  def count(cx: Connection, table: TableConfig): Int = {
    WithClose(cx.prepareStatement(s"select count(*) from ${table.name.qualified};")) { statement =>
      WithClose(statement.executeQuery()) { rs =>
        rs.next() must beTrue
        rs.getInt(1)
      }
    }
  }
}
