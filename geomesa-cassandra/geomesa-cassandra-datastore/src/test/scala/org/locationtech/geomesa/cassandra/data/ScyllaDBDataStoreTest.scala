/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.runner.JUnitRunner
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.{DockerImageName, MountableFile}

/**
 * Test suite for GeoMesa Cassandra DataStore using ScyllaDB as a drop-in replacement.
 * This test extends CassandraDataStoreTest and overrides the container definition
 * to use ScyllaDB instead of Apache Cassandra.
 *
 * Note: Uses --reactor-backend=epoll for compatibility with Docker Desktop on Mac/Windows.
 * Requires ScyllaDB 2025.1.4+ for best Docker compatibility.
 */
@RunWith(classOf[JUnitRunner])
class ScyllaDBDataStoreTest extends CassandraDataStoreTest {

  override protected def createContainer(): CassandraContainer = {
    new CassandraContainer(
      DockerImageName.parse("scylladb/scylla")
        .withTag(sys.props.getOrElse("scylladb.docker.tag", "2025.1.4"))
        .asCompatibleSubstituteFor("cassandra")
    )
      .withCommand("--reactor-backend", "epoll", "--smp", "1")
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("scylladb")))
      .withFileSystemBind(MountableFile.forClasspathResource("init.cql").getResolvedPath, "/init.cql", BindMode.READ_ONLY)
  }
}
