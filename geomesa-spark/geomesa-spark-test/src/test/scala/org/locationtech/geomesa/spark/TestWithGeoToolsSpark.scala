/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.locationtech.geomesa.spark.TestWithGeoToolsSpark.PostgisContainer
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class TestWithGeoToolsSpark extends TestWithSpark {

  val postgis: PostgisContainer =
    new PostgisContainer()
      .withNetwork(network)
      .withNetworkAliases("postgres")

  lazy val postgisParams = Map(
    "dbtype" -> "postgis",
    "host" -> postgis.getHost,
    "port" -> s"${postgis.getMappedPort(5432)}",
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> "postgres",
    "Batch insert size" -> "10",
    "preparedStatements" -> "true",
    "geotools" -> "true", // required for the geomesa spark provider
  )

  lazy val postgisSparkParams = postgisParams ++ Map("host" -> "postgres", "port" -> "5432")

  override def beforeAll(): Unit = {
    postgis.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    postgis.close()
  }
}

object TestWithGeoToolsSpark {
  val Image =
    DockerImageName.parse("ghcr.io/geomesa/postgis-cron")
      .withTag(sys.props.getOrElse("postgis.docker.tag", "15-3.4"))

  class PostgisContainer extends GenericContainer[PostgisContainer](Image) {
    withEnv("POSTGRES_PASSWORD", "postgres")
    withExposedPorts(5432)
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgis")))
    waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
  }
}
