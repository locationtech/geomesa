/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.metadata

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import java.net.URI

@RunWith(classOf[JUnitRunner])
class JdbcMetadataTest extends TestAbstractMetadata with BeforeAfterAll {

  private val container =
    new PostgreSQLContainer(DockerImageName.parse("postgres").withTag(sys.props("postgres.docker.tag")).asCompatibleSubstituteFor("postgres"))
      .withDatabaseName("postgres") // if we don't set the default db/name to postgres, the startup check fails as it restarts 3 times instead of the expected 2
      .withUsername("postgres")

  override protected val metadataType = JdbcMetadata.MetadataType

  override protected def getConfig(root: URI): Map[String, String] = {
    // the tmp dir is all numbers - change it to chars to make a valid, unique db name for each test
    val db = new String(root.toString.replace("geomesa", "").toCharArray.map(c => 'a' + c.toInt).map(_.toChar))
    WithClose(container.createConnection("")) { connection =>
      WithClose(connection.createStatement()) { statement =>
        statement.execute(s"create database $db")
      }
    }
    Map(
      JdbcMetadata.Config.UrlKey      -> container.getJdbcUrl.replace(s"/${container.getDatabaseName}", s"/$db"),
      JdbcMetadata.Config.UserKey     -> container.getUsername,
      JdbcMetadata.Config.PasswordKey -> container.getPassword,
    )
  }

  override def beforeAll(): Unit = {
    if (logger.underlying.isDebugEnabled()) {
      container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgres")))
      container.setCommand("postgres", "-c", "fsync=off", "-c", "log_statement=all")
    }
    container.start()
  }

  override def afterAll(): Unit = container.stop()
}
