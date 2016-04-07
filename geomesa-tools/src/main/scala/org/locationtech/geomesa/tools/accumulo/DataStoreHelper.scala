/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams => dsParams}
import org.locationtech.geomesa.tools.accumulo.commands.GeoMesaConnectionParams

import scala.collection.JavaConversions._

class DataStoreHelper(params: GeoMesaConnectionParams) extends AccumuloProperties {
  lazy val instance = Option(params.instance).getOrElse(instanceName)
  lazy val zookeepersString = Option(params.zookeepers).getOrElse(zookeepersProp)

  lazy val paramMap = Map[String, String](
    dsParams.instanceIdParam.getName -> instance,
    dsParams.zookeepersParam.getName -> zookeepersString,
    dsParams.userParam.getName       -> params.user,
    dsParams.passwordParam.getName   -> getPassword(params.password),
    dsParams.tableNameParam.getName  -> params.catalog,
    dsParams.visibilityParam.getName -> Option(params.visibilities).orNull,
    dsParams.authsParam.getName      -> Option(params.auths).orNull,
    dsParams.mockParam.getName       -> params.useMock.toString).filter(_._2 != null)

  /**
   * Get a handle to a datastore for a pre-existing catalog table
    *
    * @throws Exception if the catalog table does not exist in accumulo
   */
  def getDataStore() = Option(DataStoreFinder.getDataStore(paramMap).asInstanceOf[AccumuloDataStore]).getOrElse {
    throw new Exception("Could not load a data store with the provided parameters: " +
        s"${paramMap.map { case (k,v) => s"$k=$v" }.mkString(",")}")
  }
}
