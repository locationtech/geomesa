package org.locationtech.geomesa.tools.cassandra.commands

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreParams}
import org.locationtech.geomesa.tools.cassandra.CassandraConnectionParams
import org.locationtech.geomesa.tools.common.commands.CommandWithDataStore


trait CommandWithCassandraDataStore extends CommandWithDataStore {
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