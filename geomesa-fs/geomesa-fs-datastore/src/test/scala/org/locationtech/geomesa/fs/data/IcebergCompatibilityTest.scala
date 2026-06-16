/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.rest.RESTCatalog
import org.geotools.api.data.{DataStoreFinder, Transaction}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.IcebergCompatibilityTest.IcebergRestContainer
import org.locationtech.geomesa.fs.storage.core.StorageMetadata
import org.locationtech.geomesa.fs.storage.parquet.iceberg.IcebergMapper
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.{GenericContainer, MinIOContainer, Network}
import org.testcontainers.utility.DockerImageName

import java.util.HexFormat
import scala.collection.JavaConverters._
import scala.util.Random

class IcebergCompatibilityTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  private val network = Network.newNetwork()

  private val minio =
    new MinIOContainer(DockerImageName.parse("minio/minio").withTag(sys.props("minio.docker.tag")))
      .withNetwork(network)
      .withNetworkAliases("minio")

  private val rest =
    new IcebergRestContainer()
      .withNetwork(network)
      .withNetworkAliases("rest-catalog")

  private val hexFormat = HexFormat.of()

  private val sft = SimpleFeatureTypes.createType("test", "name:String:fs.bounds=true,age:Int,dtg:Date,*geom:Point:srid=4326")

  private val features = {
    val r = new Random(10)
    Seq.tabulate(10) { i =>
      val sf = ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", s"POINT(${r.nextInt(360)-180} ${r.nextInt(180)-90})")
      sf.getUserData.put("geomesa.feature.visibility", "user")
      sf
    }
  }

  private lazy val params = Map(
    "fs.path" -> s"s3://warehouse/fs/",
    "geomesa.security.auths" -> "user",
    "fs.config.properties" ->
      s"""fs.metadata.type=file
         |fs.s3.region=us-east-1
         |fs.s3.endpoint=${minio.getS3URL}
         |fs.s3.access-key-id=${minio.getUserName}
         |fs.s3.secret-access-key=${minio.getPassword}
         |fs.s3.force-path-style=true
         |""".stripMargin
  )

  private lazy val icebergConfig = Map(
    "uri" -> s"http://${rest.getHost}:${rest.getFirstMappedPort}/",
    "warehouse" -> "s3://warehouse/iceberg",
    "file-format" -> "PARQUET",
    "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
    "s3.endpoint" -> minio.getS3URL,
    "s3.access-key-id" -> minio.getUserName,
    "s3.secret-access-key" -> minio.getPassword,
    "s3.path-style-access" -> "true",
    // note: s3 analytics/crt throws dns errors with the minio endpoint, either due to localhost or the use of a port
    // "s3.analytics-accelerator.enabled" -> "true",
    // "s3.crt.enabled" -> "true",
    "client.region" -> "us-east-1",
  )

  override def beforeAll(): Unit = {
    minio.start()
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", "localhost/warehouse")
    rest.start()
  }

  override def afterAll(): Unit = {
    rest.stop()
    minio.stop()
    network.close()
  }

  "FileSystemDataStore" should {
    "be compatible with iceberg" in {
      WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        WithClose(new RESTCatalog()) { catalog =>
          catalog.initialize("geomesa", icebergConfig.asJava)
          foreach(Seq("year", "month", "day", "hour")) { time =>
            val spec = SimpleFeatureTypes.encodeType(sft) + s";geomesa.fs.scheme='$time,z2:bits=4'"
            ds.createSchema(SimpleFeatureTypes.createType(time, spec))
            WithClose(ds.getFeatureWriterAppend(time, Transaction.AUTO_COMMIT)) { writer =>
              features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
            }
            val partitions = ds.storage(time).metadata.getFiles().map(_.partition).toSet
            time match {
              case "year" =>
                partitions.map(_.values.map(_.value)) mustEqual Set(
                  Set("8000002f", "2"),
                  Set("8000002f", "3"),
                  Set("8000002f", "9"),
                  Set("8000002f", "a"),
                  Set("8000002f", "b"),
                  Set("8000002f", "c"),
                  Set("8000002f", "e"),
                )
              case "month" =>
                partitions.map(_.values.map(_.value)) mustEqual Set(
                  Set("80000239", "2"),
                  Set("80000239", "3"),
                  Set("80000239", "9"),
                  Set("80000239", "a"),
                  Set("80000239", "b"),
                  Set("80000239", "c"),
                  Set("80000239", "e"),
                )
              case "day" =>
                partitions.map(_.values.map(_.value)) mustEqual Set(
                  Set("800043ac", "2"),
                  Set("800043aa", "3"),
                  Set("800043ac", "9"),
                  Set("800043aa", "a"),
                  Set("800043ab", "a"),
                  Set("800043ac", "b"),
                  Set("800043ab", "c"),
                  Set("800043aa", "e"),
                )
              case "hour" =>
                partitions.map(_.values.map(_.value)) mustEqual Set(
                  Set("80065824", "2"),
                  Set("800657f4", "3"),
                  Set("80065824", "9"),
                  Set("800657f4", "a"),
                  Set("8006580c", "a"),
                  Set("80065824", "b"),
                  Set("8006580c", "c"),
                  Set("800657f4", "e"),
                )
            }

            val mapper = IcebergMapper(ds.storage(time).metadata.sft, ds.storage(time).metadata.schemes.toSeq.sortBy(_.name), ds.storage(time).context)
            mapper.spec.fields().asScala must haveLength(2)

            val table =
              catalog.createTable(TableIdentifier.of("geomesa", time), mapper.schema, mapper.spec, null, icebergConfig.asJava)

            val files = ds.storage(time).metadata.getFiles()
            val append = table.newAppend()
            files.map(mapper.toDataFile(table, _)).foreach(append.appendFile)
            append.commit()

            val icebergFiles = WithClose(table.newScan().planFiles())(_.asScala.map(_.file()).toList)

            icebergFiles.length mustEqual files.length

            foreach(icebergFiles) { icebergFile =>
              val file = files.find(f => ds.storage(time).context.root.resolve(f.file).toString == icebergFile.location()).orNull
              file must not(beNull)
              icebergFile.partition().get(0, classOf[java.lang.Integer]) mustEqual
                file.partition.values.collectFirst { case k if k.name.startsWith(time) => StorageMetadata.TypeRegistry.decode("integer", k.value) }.orNull
              icebergFile.partition().get(1, classOf[String]) mustEqual
                file.partition.values.collectFirst { case k if k.name.startsWith("z2") => k.value }.orNull
            }
          }
        }
      }
    }
  }
}

object IcebergCompatibilityTest {

  class IcebergRestContainer
      extends GenericContainer[IcebergRestContainer](DockerImageName.parse("tabulario/iceberg-rest").withTag(sys.props("iceberg.rest.docker.tag"))) {
    withExposedPorts(8181)
    // Override the upstream image's malformed default URI (jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory)
    // `mode=memory` ended up in the filename instead of as a query parameter.Also add a busy_timeout, so transient
    // contention from Iceberg's connection pool doesn't show up as SQLITE_BUSY 500s during multi-table ingests
    withEnv("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest.db?journal_mode=WAL&synchronous=NORMAL&busy_timeout=30000")
    withEnv("CATALOG_WAREHOUSE", "s3://warehouse/iceberg")
    withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
    withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
    withEnv("CATALOG_S3_ACCESS__KEY__ID", "minioadmin")
    withEnv("CATALOG_S3_SECRET__ACCESS__KEY", "minioadmin")
    withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    withEnv("CATALOG_S3_REGION", "us-east-1")
    withEnv("AWS_REGION", "us-east-1")
    withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
    withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
  }
}
