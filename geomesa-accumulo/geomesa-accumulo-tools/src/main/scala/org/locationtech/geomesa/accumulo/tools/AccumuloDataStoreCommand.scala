/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools

import java.io.File

import org.apache.accumulo.core.client.Connector
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait AccumuloDataStoreCommand extends DataStoreCommand[AccumuloDataStore] {

  import AccumuloDataStoreParams._


  override def params: AccumuloDataStoreParams

  override def connection: Map[String, String] = {
    if (params.mock) {
      if (params.instance == null) {
        // AccumuloDataStoreFactory requires instance ID to be set but it should not be required if mock is set...
        // so set a fake one but be careful NOT to add a mock zoo since other code will then think it has a
        // zookeeper when it doesn't
        params.instance = "mockInstance"
      }
      if (params.user == "root" && params.keytab == null) {
        // mock accumulo sets the root password to blank
        params.password = ""
      }
    }

    Map[String, String](
      InstanceIdParam.key   -> params.instance,
      ZookeepersParam.key   -> params.zookeepers,
      UserParam.key         -> params.user,
      PasswordParam.key     -> params.password,
      KeytabPathParam.key   -> params.keytab,
      CatalogParam.key      -> params.catalog,
      VisibilitiesParam.key -> params.visibilities,
      AuthsParam.key        -> params.auths,
      MockParam.key         -> params.mock.toString
    ).filter(_._2 != null)
  }
}

object AccumuloDataStoreCommand {

  trait AccumuloDistributedCommand extends AccumuloDataStoreCommand with DistributedCommand {

    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/accumulo/tools/accumulo-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_ACCUMULO_HOME", "lib"),
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HOME", "lib"), // old geomesa accumulo home path
      () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
      () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Connector])
    ) ++ super.libjarsPaths
  }
}
