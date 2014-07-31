package geomesa.tools

import geomesa.core.data.AccumuloDataStore
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.DataStoreFinder
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class Ingest() {}

object Ingest  {

  def getAccumuloDataStoreConf(config: Config): HashMap[String, Any] = {
    val dsConfig = HashMap[String, Any]()
    dsConfig.put("instanceId", config.instanceId)
    dsConfig.put("zookeepers", config.zookeepers)
    dsConfig.put("user", config.user)
    dsConfig.put("password", config.password)
    if (config.authorizations != null)  dsConfig.put("auths", config.authorizations)
    if (config.visibilities != null) dsConfig.put("visibilities", config.visibilities)
    dsConfig.put("tableName", config.table)
    val instance = new ZooKeeperInstance(config.instanceId, config.zookeepers)
    val connector = instance.getConnector(config.user, new PasswordToken(config.password))
    dsConfig.put("connector", connector)
    dsConfig
  }

  def defineIngestJob(config: Config) = {
    val dsConfig = getAccumuloDataStoreConf(config)
    println(dsConfig)
    val method = config.method
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    method match {
      case "mapreduce" => println("go go mapreduce!")
        try {
          //val sft = ds.getSchema(config.spec)
          //val spec = DataUtilities.encodeType(sft)
          println("Success")
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case "naive" =>
        try{
          println("go go naive!")
          // get the deliminator for provided type
          lazy val delim = config.format match {
            case "TSV" => "\t"
            case "CSV" => ","
          }





        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case _ => println("Error, no such method exists, no changes made")
    }
  }
}


