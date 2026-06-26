/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data.container

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.fs.data.container.FsContainerTest.IcebergRestContainer
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.{GenericContainer, MinIOContainer, Network}
import org.testcontainers.utility.DockerImageName

trait FsContainerTest extends BeforeAfterAll with LazyLogging {

  protected val network = Network.newNetwork()

  protected val minio =
    new MinIOContainer(DockerImageName.parse("minio/minio").withTag(sys.props("minio.docker.tag")))
      .withNetwork(network)
      .withNetworkAliases("minio")

  protected val iceberg =
    new IcebergRestContainer()
      .withNetwork(network)
      .withNetworkAliases("rest-catalog")

  protected lazy val s3Configs =
    s"""fs.s3.region=us-east-1
       |fs.s3.endpoint=${minio.getS3URL}
       |fs.s3.access-key-id=${minio.getUserName}
       |fs.s3.secret-access-key=${minio.getPassword}
       |fs.s3.force-path-style=true""".stripMargin

  protected lazy val dsParams = Map(
    "fs.path" -> s"s3://geomesa/fs/iceberg/",
    "geomesa.security.auths" -> "user",
    "fs.config.properties" ->
      s"""type=rest
         |uri=http://${iceberg.getHost}:${iceberg.getFirstMappedPort}/
         |iceberg.namespace=geomesa
         |# note: s3 analytics/crt throws dns errors with the minio endpoint, either due to localhost or the use of a port
         |#s3.analytics-accelerator.enabled=true
         |#s3.crt.enabled=true
         |$s3Configs
         |""".stripMargin
  )

  override def beforeAll(): Unit = {
    minio.start()
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", "localhost/geomesa")
    iceberg.start()
  }

  override def afterAll(): Unit = {
    iceberg.stop()
    minio.stop()
    network.close()
  }
}

object FsContainerTest {
  class IcebergRestContainer
      extends GenericContainer[IcebergRestContainer](DockerImageName.parse("tabulario/iceberg-rest").withTag(sys.props("iceberg.rest.docker.tag"))) {
    withExposedPorts(8181)
    // Override the upstream image's malformed default URI (jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory)
    // `mode=memory` ended up in the filename instead of as a query parameter. Also add a busy_timeout, so transient
    // contention from Iceberg's connection pool doesn't show up as SQLITE_BUSY 500s during multi-table ingests
    withEnv("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest.db?journal_mode=WAL&synchronous=NORMAL&busy_timeout=30000")
    withEnv("CATALOG_WAREHOUSE", "s3://geomesa/iceberg")
    withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
    withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
    withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    withEnv("AWS_REGION", "us-east-1")
    withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
    withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
  }
}
