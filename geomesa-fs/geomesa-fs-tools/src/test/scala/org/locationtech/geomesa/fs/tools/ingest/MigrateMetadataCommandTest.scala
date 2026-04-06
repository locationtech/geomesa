/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.FsRunner
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.nio.file.Files


@RunWith(classOf[JUnitRunner])
class MigrateMetadataCommandTest extends Specification with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  private var dir: File = _

  private val gzip = "<configuration><property><name>parquet.compression</name><value>gzip</value></property></configuration>"

  private val container =
    new PostgreSQLContainer(DockerImageName.parse("postgres").withTag(sys.props("postgres.docker.tag")).asCompatibleSubstituteFor("postgres"))
      .withDatabaseName("postgres") // if we don't set the default db/name to postgres, the startup check fails as it restarts 3 times instead of the expected 2
      .withUsername("postgres")

  private lazy val jdbcConfig =
    s"""jdbc.url=${container.getJdbcUrl}
       |jdbc.user=${container.getUsername}
       |jdbc.password=${container.getPassword}
       |""".stripMargin

  private lazy val commonParams = Map(
    "fs.path" -> dir.getPath,
    "fs.config.xml" -> gzip,
  )

  private lazy val fileParams = commonParams ++ Map(
    "fs.metadata.type" -> "file",
  )

  private lazy val jdbcParams = commonParams ++ Map(
    "fs.metadata.type" -> "jdbc",
    "fs.metadata.config" -> jdbcConfig,
  )

  private lazy val sft =
    SimpleFeatureTypes.createType("parquet", "name:String:fs.bounds=true,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.fs.scheme=daily")

  private lazy val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", s"POINT(10 10.$i)")
  }

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("fsds-test").toFile
    if (logger.underlying.isDebugEnabled()) {
      container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgres")))
      container.setCommand("postgres", "-c", "fsync=off", "-c", "log_statement=all")
    }
    container.start()
    WithClose(DataStoreFinder.getDataStore(fileParams.asJava)) { ds =>
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }
  }

  override def afterAll(): Unit = {
    if (dir != null) {
      FileUtils.deleteDirectory(dir)
    }
    container.stop()
  }

  "MigrateMetadataCommand" should {
    "migrate from file to jdbc metadata" in {
      WithClose(DataStoreFinder.getDataStore(jdbcParams.asJava)) { ds =>
        ds.getTypeNames must beEmpty
      }

      val files = WithClose(DataStoreFinder.getDataStore(fileParams.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.storage(sft.getTypeName).metadata.getFiles()
      }
      files must not(beEmpty)

      val args = Array(
        "manage-metadata", "migrate", "--path", dir.getPath, "--metadata-type", "file", "-f", sft.getTypeName,
        "--new-metadata-type", "jdbc", "--new-metadata-config", s"jdbc.url=${container.getJdbcUrl}",
        "--new-metadata-config", s"jdbc.user=${container.getUsername}",
        "--new-metadata-config", s"jdbc.password=${container.getPassword}"
      )
      FsRunner.parseCommand(args).execute()

      WithClose(DataStoreFinder.getDataStore(jdbcParams.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.getTypeNames mustEqual Array(sft.getTypeName)
        ds.storage(sft.getTypeName).metadata.getFiles() mustEqual files
        WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
          CloseableIterator(reader).toList.sortBy(_.getID) mustEqual features
        }
      }
    }
  }
}
