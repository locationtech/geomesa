package geomesa.tools

import geomesa.core.data.AccumuloDataStore
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriterConfig, ZooKeeperInstance}
import org.apache.accumulo.core.data.{Mutation, Value}
import org.geotools.data._

import scala.collection.JavaConversions._

class FeaturesTool(instanceName: String, zookeepers: String, username: String, password: String, table: String) {

  val ds: AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> instanceName,
      "zookeepers" -> zookeepers,
      "user"       -> username,
      "password"   -> password,
      "auths"      -> "A,B,C",
      "tableName"  -> table,
      "useMock"    -> "true",
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  def writeToAccumulo() {
    val instance = new ZooKeeperInstance(instanceName, zookeepers)
    val connector = instance.getConnector("root", new PasswordToken("root"))
    val batchWriter = connector.createBatchWriter("test", new BatchWriterConfig())
    val mutation = new Mutation("row")
    mutation.put("CF", "CQ", new Value("1".getBytes()))
    batchWriter.addMutation(mutation)
    batchWriter.flush()
    batchWriter.close()
  }

  def listFeatures() {
    println(ds.getNames)
  }

  def create() {
    val instance = new ZooKeeperInstance(instanceName, zookeepers)
    val connector = instance.getConnector("root", new PasswordToken("root"))
    val batchWriter = connector.createBatchWriter("test", new BatchWriterConfig())
    val mutation = new Mutation("row")
    mutation.put("CF", "CQ", new Value("1".getBytes()))
    batchWriter.addMutation(mutation)
  }
}

object FeaturesTool {}