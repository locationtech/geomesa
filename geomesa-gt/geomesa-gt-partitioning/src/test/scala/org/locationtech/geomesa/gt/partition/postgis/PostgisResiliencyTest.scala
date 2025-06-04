/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.net.ServerSocket

@RunWith(classOf[JUnitRunner])
class PostgisResiliencyTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  lazy val params = Map(
    "dbtype" -> PartitionedPostgisDataStoreParams.DbType.sample,
    "host" -> container.getHost,
    "port" -> port.toString,
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> "postgres",
    "Batch insert size" -> "10",
  )

  private val port = {
    val socket = new ServerSocket(0)
    try { socket.getLocalPort } finally {
      socket.close()
    }
  }

  private val container = new PostgisContainer("postgres", Some(port))

  if (logger.underlying.isTraceEnabled()) {
    container.withLogAllStatements()
  }

  "PartitionedPostgisDataStore" should {

    "fail to get a data store if postgres is down" in {
      DataStoreFinder.getDataStore(params.asJava) must throwAn[Exception]
    }

    "recover from a temporary database failure" in {
      container.start()
      try {
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          ds must not(beNull)
          ds.getTypeNames must beEmpty
          container.stop()
          ds.getTypeNames must throwAn[Exception]
          container.start()
          ds.getTypeNames must beEmpty
        }
      } finally {
        container.close()
      }
    }
  }
}
