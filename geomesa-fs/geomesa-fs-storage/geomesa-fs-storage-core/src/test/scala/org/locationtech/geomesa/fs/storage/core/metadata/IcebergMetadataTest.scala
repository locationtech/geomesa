/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.metadata

import org.apache.iceberg.rest.RESTCatalog
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.core.metadata.IcebergMetadataTest.IcebergRestContainer
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

import java.net.URI

@RunWith(classOf[JUnitRunner])
class IcebergMetadataTest extends TestAbstractMetadata with BeforeAfterAll {

  private val iceberg = new IcebergRestContainer().withNetwork(network)

  override def beforeAll(): Unit = {
    super.beforeAll()
    iceberg.start()
  }

  override def afterAll(): Unit = {
    iceberg.stop()
    super.afterAll()
  }

  override protected def metadataType: String = IcebergMetadata.MetadataType

  override protected def getConfig(root: URI): Map[String, String] = {
    // make a valid, unique namespace for each test
    val path = root.toString.replaceAll("[^0-9]", "")
    Map(
      "iceberg.catalog-impl" -> classOf[RESTCatalog].getName,
      "iceberg.uri" -> s"http://${iceberg.getHost}:${iceberg.getFirstMappedPort}/",
      "iceberg.namespace" -> path,
    )
  }
}

object IcebergMetadataTest {
  class IcebergRestContainer
    extends GenericContainer[IcebergRestContainer](DockerImageName.parse("tabulario/iceberg-rest").withTag(sys.props("iceberg.rest.docker.tag"))) {
    withExposedPorts(8181)
    // Override the upstream image's malformed default URI (jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory)
    // `mode=memory` ended up in the filename instead of as a query parameter. Also add a busy_timeout, so transient
    // contention from Iceberg's connection pool doesn't show up as SQLITE_BUSY 500s during multi-table ingests
    withEnv("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest.db?journal_mode=WAL&synchronous=NORMAL&busy_timeout=30000")
    withEnv("CATALOG_WAREHOUSE", "s3://geomesa/iceberg/")
    withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
    withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
    withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    withEnv("AWS_REGION", "us-east-1")
    withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
    withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
  }
}