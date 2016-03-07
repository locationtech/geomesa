/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.net.URI
import java.util
import java.util.Collections

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.geotools.util.KVP

class CassandraDataStoreFactory extends DataStoreFactorySpi {
  import CassandraDataStoreParams._

  override def createDataStore(map: util.Map[String, Serializable]): DataStore = {
    val Array(cp, port) = CONTACT_POINT.lookUp(map).asInstanceOf[String].split(":")
    val ks = KEYSPACE.lookUp(map).asInstanceOf[String]
    val ns = NAMESPACE.lookUp(map).asInstanceOf[URI]
    val cluster =
      Cluster.builder()
        .addContactPoint(cp)
        .withPort(port.toInt)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .build()
    val session = cluster.connect(ks)
    new CassandraDataStore(session, cluster.getMetadata.getKeyspace(ks), ns)
  }

  override def createNewDataStore(map: util.Map[String, Serializable]): DataStore = createDataStore(map)

  override def getDisplayName: String = "Cassandra (GeoMesa)"

  override def getDescription: String = "GeoMesa Cassandra Data Store"

  override def getParametersInfo: Array[Param] = CassandraDataStoreParams.PARAMS

  override def isAvailable: Boolean = true

  override def canProcess(params: util.Map[String, Serializable]): Boolean = canProcessCassandra(params)

  override def getImplementationHints: util.Map[Key, _] = Collections.emptyMap()
}

object CassandraDataStoreParams {

  val CONTACT_POINT = new Param("geomesa.cassandra.contact.point"  , classOf[String], "HOST:PORT to Cassandra",   true)
  val KEYSPACE      = new Param("geomesa.cassandra.keyspace"       , classOf[String], "Cassandra Keyspace", true)
  val NAMESPACE     = new Param("namespace", classOf[URI], "uri to a the namespace", false, null, new KVP(Parameter.LEVEL, "advanced"))

  val PARAMS  = Array(CONTACT_POINT, KEYSPACE, NAMESPACE)

  def canProcessCassandra(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey(CONTACT_POINT.key) && params.containsKey(KEYSPACE.key)
  }
}
