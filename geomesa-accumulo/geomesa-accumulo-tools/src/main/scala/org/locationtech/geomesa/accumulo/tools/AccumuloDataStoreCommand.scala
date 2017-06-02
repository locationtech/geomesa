/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools

import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.DataStoreCommand

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait AccumuloDataStoreCommand extends DataStoreCommand[AccumuloDataStore] {

  // AccumuloDataStoreFactory requires instance ID to be set but it should not be required if mock is set...so set a
  // fake one but be careful NOT to add a mock zoo since other code will then think it has a zookeeper but doesn't
  lazy private val mockDefaults = Map[String, String](AccumuloDataStoreParams.instanceIdParam.getName -> "mockInstance")

  override def params: AccumuloDataStoreParams

  override def connection: Map[String, String] = {
    val parsedParams = Map[String, String](
      AccumuloDataStoreParams.instanceIdParam.getName -> params.instance,
      AccumuloDataStoreParams.zookeepersParam.getName -> params.zookeepers,
      AccumuloDataStoreParams.userParam.getName       -> params.user,
      AccumuloDataStoreParams.passwordParam.getName   -> params.password,
      AccumuloDataStoreParams.keytabPathParam.getName -> params.keytab,
      AccumuloDataStoreParams.tableNameParam.getName  -> params.catalog,
      AccumuloDataStoreParams.visibilityParam.getName -> params.visibilities,
      AccumuloDataStoreParams.authsParam.getName      -> params.auths,
      AccumuloDataStoreParams.mockParam.getName       -> params.mock.toString
    ).filter(_._2 != null)

    if (parsedParams.get(AccumuloDataStoreParams.mockParam.getName).exists(_.toBoolean)) {
      val ret = mockDefaults ++ parsedParams // anything passed in will override defaults
      // MockAccumulo sets the root password to blank so we must use it if user is root, providing not using keytab instead
      if (ret(AccumuloDataStoreParams.userParam.getName) == "root" &&
        !(ret contains AccumuloDataStoreParams.keytabPathParam.getName)) {
        ret.updated(AccumuloDataStoreParams.passwordParam.getName, "")
      } else {
        ret
      }
    } else {
      parsedParams
    }
  }
}
