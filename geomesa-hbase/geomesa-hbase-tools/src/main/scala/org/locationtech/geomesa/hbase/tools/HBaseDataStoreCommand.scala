/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools

import java.io.File
import java.util.Collections

import com.beust.jcommander.Parameter
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.locationtech.geomesa.hbase.data.{HBaseConnectionPool, HBaseDataStore, HBaseDataStoreParams}
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.HBaseParams
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, DistributedCommand, OptionalZookeepersParam}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait HBaseDataStoreCommand extends DataStoreCommand[HBaseDataStore] {

  override def params: HBaseParams

  override def connection: Map[String, String] = {
    // set zookeepers explicitly, so that if the hbase-site.xml isn't distributed with jobs we can still connect
    val zk = if (params.zookeepers != null) { params.zookeepers } else {
      HBaseConnectionPool.getConfiguration(Collections.emptyMap()).get(HConstants.ZOOKEEPER_QUORUM)
    }
    Map(
      HBaseDataStoreParams.ZookeeperParam.getName       -> zk,
      HBaseDataStoreParams.HBaseCatalogParam.getName    -> params.catalog,
      HBaseDataStoreParams.RemoteFilteringParam.getName -> (!params.noRemote).toString,
      HBaseDataStoreParams.EnableSecurityParam.getName  -> params.secure.toString,
      HBaseDataStoreParams.AuthsParam.getName           -> params.auths
    ).filter(_._2 != null)
  }
}

object HBaseDataStoreCommand {

  trait HBaseDistributedCommand extends HBaseDataStoreCommand with DistributedCommand {

    // TODO need to pass hbase-site.xml around
    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/hbase/tools/hbase-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HBASE_HOME", "lib"),
      () => ClassPathUtils.getJarsFromEnvironment("HBASE_HOME"),
      () => ClassPathUtils.getJarsFromClasspath(classOf[HBaseDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Connection])
    ) ++ super.libjarsPaths
  }

  trait HBaseParams extends CatalogParam with OptionalZookeepersParam with RemoteFilterParam {
    @Parameter(names = Array("--secure"), description = "Enable HBase security (visibilities)")
    var secure: Boolean = false

    @Parameter(names = Array("--authorizations"), description = "Authorizations used for querying, comma-delimited")
    var auths: String = _
  }

  /**
    * Disables remote filtering/coprocessors
    */
  trait RemoteFilterParam {
    def noRemote: Boolean
  }

  /**
    * Exposes remote filtering as a command argument
    */
  trait ToggleRemoteFilterParam extends RemoteFilterParam {
    @Parameter(names = Array("--no-remote-filters"), description = "Disable remote filtering and coprocessors", arity = 0)
    var noRemote: Boolean = false
  }

  /**
    * Doesn't expose filtering as a command argument, for commands that wouldn't use filtering anyway.
    * Note that this means remote filtering is enabled.
    */
  trait RemoteFilterNotUsedParam extends RemoteFilterParam {
    override def noRemote: Boolean = false
  }
}
