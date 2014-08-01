package geomesa.tools

import geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader}
import geomesa.core.integration.data.{DataExporter, LoadAttributes}
import geomesa.core.index.{IndexSchemaBuilder, SF_PROPERTY_START_TIME}
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.text.WKTUtils
import org.geotools.data._
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class FeaturesTool(catalogTable: String) {
  val user = sys.env.getOrElse("GEOMESA_USER", "admin")
  val password = sys.env.getOrElse("GEOMESA_PASSWORD", "admin")
  val instanceId = sys.env.getOrElse("GEOMESA_INSTANCEID", "instanceId")
  val zookeepers = sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "zoo1:2181,zoo2:2181,zoo3:2181")
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

  def exportFeatures(feature: String,
                      attributes: String,
                      idAttribute: String,
                      latAttribute: Option[String],
                      lonAttribute: Option[String],
                      dateAttribute: Option[String],
                      format: String,
                      query: String) {
    val loadAttributes = new LoadAttributes(feature, table, attributes, idAttribute, latAttribute, lonAttribute, dateAttribute, query)
    val de = new DataExporter(loadAttributes, Map(
      "instanceId" -> instanceId,
      "zookeepers" -> zookeepers,
      "user"       -> user,
      "password"   -> password,
      "tableName"  -> table,
      "featureEncoding" -> "avro"), format)
    de.writeFeatures(de.queryFeatures())
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