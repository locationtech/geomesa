package geomesa.tools

import geomesa.core.data.AccumuloDataStore
import org.geotools.data._

import scala.collection.JavaConversions._

class ExportTool {
  def createStore(table: String): AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"       -> "root",
      "password"   -> "secret",
      "auths"      -> "A,B,C",
      "tableName"  -> table,
      "useMock"    -> "true",
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
  }
}

object ExportTool {}