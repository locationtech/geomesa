/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.tools

import java.io.File

import org.apache.hadoop.hbase.client.Connection
import org.locationtech.geomesa.bigtable.data.BigtableDataStoreFactory
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseDataStoreParams}
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.tools.DistributedCommand
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

trait BigtableDataStoreCommand extends HBaseDataStoreCommand {
  override def connection: Map[String, String] = {
    Map(
      BigtableDataStoreFactory.BigtableCatalogParam.getName -> params.catalog,
      HBaseDataStoreParams.ZookeeperParam.getName           -> params.zookeepers,
      HBaseDataStoreParams.RemoteFilteringParam.getName     -> "false",
      HBaseDataStoreParams.EnableSecurityParam.getName      -> params.secure.toString,
      HBaseDataStoreParams.AuthsParam.getName               -> params.auths
    ).filter(_._2 != null)
  }
}

object BigtableDataStoreCommand {

  trait BigtableDistributedCommand extends BigtableDataStoreCommand with DistributedCommand {

    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/bigtable/tools/bigtable-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_BIGTABLE_HOME", "lib"),
      () => ClassPathUtils.getJarsFromClasspath(classOf[HBaseDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Connection])
    ) ++ super.libjarsPaths
  }
}
