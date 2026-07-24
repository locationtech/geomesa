/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.trino.datastore.{TrinoDataStore, TrinoDataStoreFactory}
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand.TrinoParams
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import java.io.File

/**
 * Abstract class for TrinoDataStore commands
 */
trait TrinoDataStoreCommand extends DataStoreCommand[TrinoDataStore] {

  override def params: TrinoParams

  override def connection: Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    builder += (TrinoDataStoreFactory.HOST.key -> params.host)
    builder += (TrinoDataStoreFactory.PORT.key -> params.port.toString)
    if (params.schema != null) {
      builder += (TrinoDataStoreFactory.SCHEMA.key -> params.schema)
    }
    if (params.user != null) {
      builder += (TrinoDataStoreFactory.USER.key -> params.user)
    }
    if (params.secret != null) {
      builder += (TrinoDataStoreFactory.SECRET.key -> params.secret)
    }
    if (params.auths != null) {
      builder += (TrinoDataStoreFactory.AUTHS.key -> params.auths)
    }
    builder.result()
  }
}

object TrinoDataStoreCommand {

  trait TrinoDistributedCommand extends TrinoDataStoreCommand with DistributedCommand {

    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/trino/tools/trino-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_TRINO_HOME", "lib"),
      () => ClassPathUtils.getJarsFromClasspath()
    ) ++ super.libjarsPaths
  }

  trait TrinoParams {
    @Parameter(names = Array("--host", "-h"), description = "Trino coordinator host", required = true)
    var host: String = _

    @Parameter(names = Array("--port", "-p"), description = "Trino coordinator port", required = true)
    var port: Integer = _

    @Parameter(names = Array("--schema"), description = "Trino schema")
    var schema: String = _

    @Parameter(names = Array("--user"), description = "Trino connection user")
    var user: String = _

    @Parameter(names = Array("--secret"), description = "Trino shared secret credential")
    var secret: String = _

    @Parameter(names = Array("--auths"), description = "Authorizations used to read data")
    var auths: String = _
  }
}
