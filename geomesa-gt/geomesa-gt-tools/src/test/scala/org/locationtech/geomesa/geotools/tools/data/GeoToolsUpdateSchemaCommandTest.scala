/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.jdbc.JDBCDataStore
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.gt.partition.postgis.PartitionedPostgisDataStoreParams
import org.locationtech.geomesa.gt.partition.postgis.dialect.TypeInfo
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures.{PartitionMaintenance, RollWriteAheadLog}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.images.builder.ImageFromDockerfile

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class GeoToolsUpdateSchemaCommandTest extends Specification with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  val sft =
    SimpleFeatureTypes.createType(
      "tools",
      "name:String,dtg:Date,*geom:Point:srid=4326;" +
          "pg.partitions.interval.hours=6," +
          "pg.partitions.cron.minute=1")
  val features = List.tabulate(24) { i =>
    val dtg = f"2023-10-30T0$i%02d:00:00.000Z"
    val sf = ScalaSimpleFeature.create(sft, f"${sft.getTypeName}.id$i%02d", "name" + i, dtg, s"POINT(0 $i)")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf.getUserData.put(Hints.PROVIDED_FID, f"$i%02d")
    sf
  }

  val schema = "public"

  lazy val now = System.currentTimeMillis()

  lazy val dsParams = Map(
    "dbtype" -> PartitionedPostgisDataStoreParams.DbType.sample.toString,
    "host" -> host,
    "port" -> port,
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> "postgres",
    "Batch insert size" -> "10",
    "preparedStatements" -> "true"
  )

  var container: GenericContainer[_] = _

  lazy val host = Option(container).map(_.getHost).getOrElse("localhost")
  lazy val port = Option(container).map(_.getFirstMappedPort).getOrElse(5432).toString

  override def beforeAll(): Unit = {
    val image =
      new ImageFromDockerfile("testcontainers/postgis_cron", false)
          .withFileFromClasspath(".", "testcontainers")
          .withBuildArg("FROM_TAG", sys.props.getOrElse("postgis.docker.tag", "15-3.3"))
    container = new GenericContainer(image)
    container.addEnv("POSTGRES_HOST_AUTH_METHOD", "trust")
    container.addExposedPort(5432)
    container.start()
    container.followOutput(new Slf4jLogConsumer(logger.underlying))
  }

  override def afterAll(): Unit = {
    if (container != null) {
      container.stop()
    }
  }

  "GeoToolsUpdateSchemaCommand" should {
    "support schema updates" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { case ds: JDBCDataStore =>
        ds must not(beNull)
        ds.getSchema(sft.getTypeName) must throwAn[Exception]
        ds.createSchema(sft)

        val typeInfo = TypeInfo(this.schema, sft)

        def write(features: Seq[SimpleFeature]): Unit = {
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _))
          }
          // manually run the partition jobs
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
            WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
          }
        }

        def getFeatures: Seq[SimpleFeature] = {
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            SelfClosingIterator(reader).map(ScalaSimpleFeature.copy).toList.sortBy(_.getID)
          }
        }

        // get all the tables associated with the schema
        def getTables: Seq[String] = {
          val tables = ArrayBuffer.empty[String]
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.getMetaData.getTables(null, null, sft.getTypeName + "_partition%", Array("TABLE"))) { rs =>
              while (rs.next()) {
                tables += rs.getString(3)
              }
            }
          }
          tables.toSeq.sorted
        }

        // get the cron schedule for the partition maintenance job
        def getCron: String = {
          var cron: String = null
          val sql = s"select schedule from cron.job where command like 'CALL ${PartitionMaintenance.name(typeInfo).quoted}()'"
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.prepareStatement(sql)) { st =>
              WithClose(st.executeQuery()) { rs =>
                rs.next() must beTrue
                cron = rs.getString(1).trim
                rs.next() must beFalse
              }
            }
          }
          cron
        }

        getTables must beEmpty

        // write the first 12 features
        write(features.take(12))

        // verify 6 hour partitions
        getTables mustEqual Seq("tools_partition_2023_10_30_00", "tools_partition_2023_10_30_06")

        // verify features come back ok - note we have to compare backwards due to Date vs Timestamp equality
        features.take(12) mustEqual getFeatures

        // verify cron schedule that we configured above
        getCron mustEqual "1,11,21,31,41,51 * * * *"

        val command = new GeoToolsUpdateSchemaCommand() {
          override lazy val connection: Map[String, String] = dsParams
        }
        command.params.featureName = sft.getTypeName
        command.params.force = true

        // run the schema update command
        command.params.userData = java.util.Arrays.asList("pg.partitions.interval.hours:2", "pg.partitions.cron.minute:2")
        command.execute()

        // verify cron schedule was updated
        getCron mustEqual "2,12,22,32,42,52 * * * *"

        // write the next 12 features
        write(features.drop(12))

        // verify partitions changed to 2 hours
        getTables mustEqual
            Seq(
              "tools_partition_2023_10_30_00",
              "tools_partition_2023_10_30_06",
              "tools_partition_2023_10_30_12",
              "tools_partition_2023_10_30_14",
              "tools_partition_2023_10_30_16",
              "tools_partition_2023_10_30_18",
              "tools_partition_2023_10_30_20",
              "tools_partition_2023_10_30_22"
            )

        // verify features still come back ok
        features mustEqual getFeatures

        // add an age-off
        // note: since the data is from the past, this will age-off the entire dataset
        command.params.userData = java.util.Arrays.asList("pg.partitions.max:4")
        command.execute()

        // run the age-off job
        write(Seq.empty)

        // verify partitions were dropped
        getTables must beEmpty

        // verify features were dropped
        getFeatures must beEmpty
      }
    }
  }
}
