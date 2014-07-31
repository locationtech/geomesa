package geomesa.tools

import com.typesafe.config.ConfigFactory
import geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader}
//import geomesa.core.integration.data.{DataExporter, LoadAttributes}
import geomesa.utils.geotools.SimpleFeatureTypes
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL

import scala.collection.JavaConversions._

class FeaturesTool(catalogTable: String) {
  val conf = ConfigFactory.load()
  val user = conf.getString("tools.user")
  val password = conf.getString("tools.password")
  val instanceId = conf.getString("tools.instanceId")
  val zookeepers = conf.getString("tools.zookeepers")
  val table = catalogTable

  val ds: AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> instanceId,
      "zookeepers" -> zookeepers,
      "user"       -> user,
      "password"   -> password,
      "tableName"  -> table,
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  def listFeatures() {
    ds.getTypeNames foreach {name =>
      println(s"$name: ${ds.getSchema(name).getAttributeDescriptors}")
    }
  }

  def createFeatureType(sftName: String, sftString: String): Boolean = {
    val sft = SimpleFeatureTypes.createType(sftName, sftString)
    ds.createSchema(sft)
    if (ds.getSchema(sftName) != null) {
      true
    } else false
  }

  def exportFeatures(feature: String, attributes: String, idAttribute: String, latAttribute: Option[String], lonAttribute: Option[String], dateAttribute: Option[String], query: String) {
//    val loadAttributes = new LoadAttributes(feature, table, attributes, idAttribute, latAttribute, lonAttribute, dateAttribute, query)
//    val de = new DataExporter(loadAttributes, Map(
//      "instanceId" -> instanceId,
//      "zookeepers" -> zookeepers,
//      "user"       -> user,
//      "password"   -> password,
//      "tableName"  -> table,
//      "featureEncoding" -> "avro"))
//    de.writeFeatures(de.queryFeatures())
  }

  def deleteFeature(sftName: String): Boolean = {
    ds.deleteSchema(sftName)
    if (!ds.getNames.contains(sftName)) {
      true
    } else false
  }

  def explainQuery(featureName: String, filterString: String) = {
    val q = new Query()
    val t = Transaction.AUTO_COMMIT
    q.setTypeName(featureName)

    val f = ECQL.toFilter(filterString)
    q.setFilter(f)

    val afr = ds.getFeatureReader(q, t).asInstanceOf[AccumuloFeatureReader]

    afr.explainQuery(q)
  }
}

object FeaturesTool {}