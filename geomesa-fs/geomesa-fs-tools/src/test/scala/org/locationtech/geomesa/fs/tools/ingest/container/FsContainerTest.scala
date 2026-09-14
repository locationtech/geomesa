/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest.container

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.fs.tools.ingest.container.FsContainerTest.{IcebergRestContainer, SeaweedFsContainer}
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.utility.DockerImageName

trait FsContainerTest extends BeforeAfterAll with LazyLogging {

  protected val network = Network.newNetwork()

  protected val s3 = new SeaweedFsContainer().withNetwork(network)

  protected val iceberg =
    new IcebergRestContainer()
      .withNetwork(network)
      .withNetworkAliases("rest-catalog")

  protected lazy val s3Configs = Map(
    "fs.s3.region" -> s"us-east-1",
    "fs.s3.endpoint" -> s3.getS3URL,
    "fs.s3.access-key-id" -> "admin",
    "fs.s3.secret-access-key" -> "admin",
    "fs.s3.force-path-style" -> "true",
  )

  protected lazy val configs = s3Configs ++ Map(
    "type" -> "rest",
    "uri" -> s"http://${iceberg.getHost}:${iceberg.getFirstMappedPort}/",
    "iceberg.namespace" -> "geomesa",
  )

  protected lazy val dsParams = Map(
    "geomesa.security.auths" -> "user",
    "fs.config.properties" -> configs.map { case (k, v) => s"$k=$v" }.mkString("\n"),
  )

  override def beforeAll(): Unit = {
    s3.start()
    iceberg.start()
  }

  override def afterAll(): Unit = {
    iceberg.stop()
    s3.stop()
    network.close()
  }
}

object FsContainerTest {

  val IcebergRestImage = DockerImageName.parse("apache/iceberg-rest-fixture").withTag(sys.props("iceberg.rest.docker.tag"))

  class IcebergRestContainer extends GenericContainer[IcebergRestContainer](IcebergRestImage) {
    withExposedPorts(8181)
    withEnv("CATALOG_WAREHOUSE", "s3://geomesa/iceberg/")
    withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
    withEnv("CATALOG_S3_ENDPOINT", "http://seaweed:8333")
    withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    withEnv("AWS_REGION", "us-east-1")
    withEnv("AWS_ACCESS_KEY_ID", "admin")
    withEnv("AWS_SECRET_ACCESS_KEY", "admin")
  }

  val SeaweedFsImage = DockerImageName.parse("chrislusf/seaweedfs").withTag(sys.props("seaweed.docker.tag"))

  class SeaweedFsContainer extends GenericContainer[SeaweedFsContainer](SeaweedFsImage) {
    withExposedPorts(8333)
    withCommand("mini", "-dir=/tmp/data")
    withEnv("AWS_ACCESS_KEY_ID", "admin")
    withEnv("AWS_SECRET_ACCESS_KEY", "admin")
    withEnv("S3_BUCKET", "geomesa")
    waitingFor(Wait.forHttp("/status").forPort(8333).forStatusCode(200))
    withNetworkAliases("seaweed")

    def getS3URL: String = s"http://$getHost:$getFirstMappedPort/"
  }
}
