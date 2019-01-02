/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2019 Dstl
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
  lazy private val mockDefaults = Map[String, String](AccumuloDataStoreParams.InstanceIdParam.getName -> "mockInstance")

  override def params: AccumuloDataStoreParams

  override def connection: Map[String, String] = {
    import scala.collection.JavaConversions._
    val parsedParams = Map[String, String](
      AccumuloDataStoreParams.InstanceIdParam.getName   -> params.instance,
      AccumuloDataStoreParams.ZookeepersParam.getName   -> params.zookeepers,
      AccumuloDataStoreParams.UserParam.getName         -> params.user,
      AccumuloDataStoreParams.PasswordParam.getName     -> params.password,
      AccumuloDataStoreParams.KeytabPathParam.getName   -> params.keytab,
      AccumuloDataStoreParams.CatalogParam.getName      -> params.catalog,
      AccumuloDataStoreParams.VisibilitiesParam.getName -> params.visibilities,
      AccumuloDataStoreParams.AuthsParam.getName        -> params.auths,
      AccumuloDataStoreParams.MockParam.getName         -> params.mock.toString
    ).filter(_._2 != null)

    if (AccumuloDataStoreParams.MockParam.lookup(parsedParams)) {
      val ret = mockDefaults ++ parsedParams // anything passed in will override defaults
      // MockAccumulo sets the root password to blank so we must use it if user is root, providing not using keytab instead
      if (AccumuloDataStoreParams.UserParam.lookup(ret) == "root" &&
          !AccumuloDataStoreParams.KeytabPathParam.exists(ret)) {
        ret.updated(AccumuloDataStoreParams.PasswordParam.getName, "")
      } else {
        ret
      }
    } else {
      parsedParams
    }
  }
}
