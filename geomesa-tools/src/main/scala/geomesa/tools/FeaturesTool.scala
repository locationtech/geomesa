package geomesa.tools

import com.typesafe.config.ConfigFactory
import geomesa.core.data.AccumuloDataStore
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriterConfig, ZooKeeperInstance}
import org.apache.accumulo.core.data.{Mutation, Value}
import org.geotools.data._

import scala.collection.JavaConversions._

class FeaturesTool(table: String) {
  val conf = ConfigFactory.load()
  val user = conf.getString("tools.user")
  val password = conf.getString("tools.password")
  val instanceId = conf.getString("tools.instanceId")
  val zookeepers = conf.getString("tools.zookeepers")
  val auths = conf.getString("auths")
  val visibilities = conf.getString("visibilities")

  val ds: AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> instanceId,
      "zookeepers" -> zookeepers,
      "user"       -> user,
      "password"   -> password,
      "tableName"  -> table,
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
  }

//  def writeToAccumulo() {
//    val instance = new ZooKeeperInstance(instanceId, zookeepers)
//    val connector = instance.getConnector("root", new PasswordToken("root"))
//    val batchWriter = connector.createBatchWriter("test", new BatchWriterConfig())
//    val mutation = new Mutation("row")
//    mutation.put("CF", "CQ", new Value("1".getBytes()))
//    batchWriter.addMutation(mutation)
//    batchWriter.flush()
//    batchWriter.close()
//  }

  def listFeatures() {
    println(ds.getNames)
  }

  def createFeatures() {
    val instance = new ZooKeeperInstance(instanceId, zookeepers)
    val connector = instance.getConnector("root", new PasswordToken("root"))
    val batchWriter = connector.createBatchWriter("test", new BatchWriterConfig())
    val mutation = new Mutation("row")
    mutation.put("CF", "CQ", new Value("1".getBytes()))
    batchWriter.addMutation(mutation)
  }

  def exportFeatures() {

  }
}

object FeaturesTool {}