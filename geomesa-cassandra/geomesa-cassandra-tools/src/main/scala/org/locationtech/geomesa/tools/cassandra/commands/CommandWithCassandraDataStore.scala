/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.cassandra.commands

import com.beust.jcommander.JCommander
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreParams}
import org.locationtech.geomesa.tools.cassandra.CassandraConnectionParams
import org.locationtech.geomesa.tools.common.commands.{Command, CommandWithDataStore}


abstract class CommandWithCassandraDataStore(parent: JCommander) extends Command(parent) with CommandWithDataStore {
  val params: CassandraConnectionParams
  lazy val ds = CassandraDataStoreParamsHelper.createDataStore(params)
}


object CassandraDataStoreParamsHelper {

  def getDataStoreParams(params: CassandraConnectionParams): Map[String, String] = {
    Map[String, String](
      CassandraDataStoreParams.CONTACT_POINT.getName -> params.contactPoint,
      CassandraDataStoreParams.KEYSPACE.getName -> params.keySpace,
      CassandraDataStoreParams.NAMESPACE.getName -> params.nameSpace
    )
  }

  def createDataStore(params: CassandraConnectionParams): CassandraDataStore = {
    val dataStoreParams = getDataStoreParams(params)
    import scala.collection.JavaConversions._
    Option(DataStoreFinder.getDataStore(dataStoreParams).asInstanceOf[CassandraDataStore]).getOrElse {
      throw new IllegalArgumentException("Could not load a data store with the provided parameters: " +
        dataStoreParams.map { case (k,v) => s"$k=$v" }.mkString(","))
    }
  }
}
