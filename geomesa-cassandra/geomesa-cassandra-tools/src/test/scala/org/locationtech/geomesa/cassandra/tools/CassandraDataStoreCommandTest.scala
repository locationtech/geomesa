/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools

import com.beust.jcommander.JCommander
import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.cassandra.tools.commands.CassandraGetTypeNamesCommand
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CassandraDataStoreCommandTest extends Specification {

  private val required =
    Seq("-P", "localhost:9042", "--local-datacenter", "datacenter1", "-k", "geomesa", "-c", "test")

  private def connection(args: String*): Map[String, String] = {
    val command = new CassandraGetTypeNamesCommand()
    JCommander.newBuilder().addObject(command.params).build().parse(required ++ args: _*)
    command.connection
  }

  "CassandraDataStoreCommand" should {
    "pass through the connection parameters" in {
      val params = connection()
      params.get(Params.ContactPointParam.getName) must beSome("localhost:9042")
      params.get(Params.LocalDatacenterParam.getName) must beSome("datacenter1")
      params.get(Params.KeySpaceParam.getName) must beSome("geomesa")
      params.get(Params.CatalogParam.getName) must beSome("test")
    }
    "not set the query consistency by default" in {
      connection().get(Params.QueryConsistencyParam.getName).flatMap(Option.apply) must beNone
    }
    "pass through the query consistency" in {
      connection("--consistency", "LOCAL_QUORUM").get(Params.QueryConsistencyParam.getName) must beSome("LOCAL_QUORUM")
    }
  }
}
