/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import scala.collection.mutable.ArrayBuffer

/**
 * Helper for a postgis container
 *
 * @param password postgres user password
 * @param port port to bind to, or None for a random port
 */
class PostgisContainer(password: String, port: Option[Int] = None)
  extends GenericContainer[PostgisContainer](PostgisContainer.Image) {

  private val command = ArrayBuffer[String]("postgres")

  port match {
    case None => addExposedPort(5432)
    case Some(p) =>
      addFixedExposedPort(p, p)
      withPgConf("port", p.toString)
  }

  withEnv("POSTGRES_PASSWORD", password)
  withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgis")))
  waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))

  /**
   * Log all database statements
   *
   * @return
   */
  def withLogAllStatements(): PostgisContainer = withPgConf("log_statement", "all")

  /**
   * Set a postgresql.conf value
   *
   * @param key key
   * @param value value
   * @return
   */
  def withPgConf(key: String, value: String): PostgisContainer = {
    command += "-c"
    command += s"$key=$value"
    setCommand(command: _*)
    this
  }
}

object PostgisContainer {
  val Image =
    DockerImageName.parse("ghcr.io/geomesa/postgis-cron")
      .withTag(sys.props.getOrElse("postgis.docker.tag", "15-3.4"))
}
