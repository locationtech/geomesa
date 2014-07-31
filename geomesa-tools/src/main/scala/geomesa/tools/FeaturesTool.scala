package geomesa.tools

import com.typesafe.config.ConfigFactory
import geomesa.core.data.AccumuloDataStore
import geomesa.utils.geotools.SimpleFeatureTypes
import org.geotools.data._

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

  def exportFeatures() {
//    val instance = new ZooKeeperInstance(instanceId, zookeepers)
//    val connector = instance.getConnector(user, new PasswordToken(password))
//    val batchWriter = connector.createBatchWriter(table, new BatchWriterConfig())
//    val mutation = new Mutation("row")
//    mutation.put("CF", "CQ", new Value("1".getBytes()))
//    batchWriter.addMutation(mutation)
  }

  def deleteFeature(sftName: String): Boolean = {
    ds.deleteSchema(sftName)
    if (!ds.getNames.contains(sftName)) {
      true
    } else false
  }
}

object FeaturesTool {}