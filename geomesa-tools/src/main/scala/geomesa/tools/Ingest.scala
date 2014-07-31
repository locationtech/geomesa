package geomesa.tools

import com.typesafe.config.ConfigFactory
import geomesa.core.data.AccumuloDataStore
import geomesa.core.index.Constants
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{DataStoreFinder, DataUtilities}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class Ingest() {}

object Ingest  {
  val conf = ConfigFactory.load()
  val user = conf.getString("tools.user")
  val password = conf.getString("tools.password")
  val instanceId = conf.getString("tools.instanceId")
  val zookeepers = conf.getString("tools.zookeepers")
  val auths = conf.getString("auths")
  val visibilities = conf.getString("visibilities")

  def getAccumuloDataStoreConf(config: Config): HashMap[String, Any] = {
    val dsConfig = HashMap[String, Any]()
    dsConfig.put("instanceId", config.instanceId)
    dsConfig.put("zookeepers", config.zookeepers)
    dsConfig.put("user", config.user)
    dsConfig.put("password", config.password)
    if (config.authorizations == null) { dsConfig.put("auths", "")
    } else { dsConfig.put("auths", config.authorizations) }
    dsConfig.put("tableName", config.table)
    dsConfig
  }

  def defineIngestJob(config: Config) = {
    val dsConfig = getAccumuloDataStoreConf(config)
    println(dsConfig)
    val instance = new ZooKeeperInstance(instanceId, zookeepers)
    val connector = instance.getConnector(user, new PasswordToken(password))
    dsConfig.put("connector", connector)
    val method = config.method
    method match {
      case "mapreduce" => println("go go mapreduce!")
        try {
          val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore] //what about connector, or is it not needed?
          //if (ds == null) throw new IllegalArgumentException(" Data Store was not found. Ending ")
          val sft = ds.getSchema(config.spec)
          val dtgTargetField = sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
          val spec = DataUtilities.encodeType(sft)
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case "naive" => println("go go naive!")
      case _ => println("Error, no such method exists, no changes made")
    }
  }
}


