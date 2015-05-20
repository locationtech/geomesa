package org.locationtech.geomesa.tools.ingest

import com.twitter.scalding._
import com.vividsolutions.jts.geom.Coordinate
import org.apache.hadoop.conf.Configuration
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import scala.collection.JavaConversions._

class Ing(args: Args) extends Job(args) {



  class R {
    lazy val sft = SimpleFeatureTypes.createType("frz3", "dtg:Date,jsonrowid:String,registration:String,lat:Double,lon:Double,trackingdegrees:Integer,altitude:Integer,speedkt:Integer,squawk:String,radarid:String,planetype:String,tailname:String,datatimestamp:Long,departure:String,arrival:String,callsign:String:index=full,possibleonground:Boolean,vertspeed:Integer,callsign2:String,unknown1:Integer,*geom:Point:srid=4326:index=full:index-value=true")
    lazy val (ds, fw) = {
      val retds = DataStoreFinder.getDataStore(
        Map(
          "instanceId" -> "ds",
          "zookeepers"   -> "zoo1,zoo2,zoo3",
          "tableName" -> "frz3kryo",
          "user"->"root",
          "password"->"secret"))

      if(retds.getSchema("frz3") == null) {
        retds.createSchema(sft)
      }
      val retfw = retds.getFeatureWriterAppend("frz3", Transaction.AUTO_COMMIT)
      (retds, retfw)
    }

    def release(): Unit = {
      if(fw != null) fw.close()
    }

    def conv(t: String): String => AnyRef = t match {
      case "Date" => s => (new DateTime(s)).toDate
      case "Double" => s => java.lang.Double.valueOf(s)
      case "Integer" => s => Integer.valueOf(s)
      case "Long" => s => java.lang.Long.valueOf(s)
      case "String" => s => s
      case _ => s => s
    }

    lazy val converters =
      "dtg:Date,jsonrowid:String,registration:String:index=full,lat:Double,lon:Double,trackingdegrees:Integer,altitude:Integer,speedkt:Integer,squawk:String:index=join,radarid:String,planetype:String,tailname:String,datatimestamp:Long,departure:String,arrival:String,callsign:String,possibleonground:Boolean,vertspeed:Integer,callsign2:String,unknown1:Integer,*geom:Point:srid=4326:index=full:index-value=true".split(",").map(_.split(":").apply(1)).map { s => conv(s) }

    lazy val gf = JTSFactoryFinder.getGeometryFactory
    import org.opengis.feature.simple.SimpleFeature
    def setGeom(f: SimpleFeature) = {
      val pt = gf.createPoint(new Coordinate(f.getAttribute("lon").asInstanceOf[Double], f.getAttribute("lat").asInstanceOf[Double]))
      f.setDefaultGeometry(pt)
      f
    }

    def parseF(l: String): Array[AnyRef] = {
      l.split(",").map(_.replace("\"", "")).zip(converters).map { case (v, f) => f(v) }
    }

    def processLine(line: String): Unit =
      try {
        val f = fw.next()
        val attrs = parseF(line)
        f.setAttributes(attrs)
        setGeom(f)
        fw.write()
      } catch {
        case _: Throwable => // nothing
      }
  }

  TextLine("hdfs://hd:54310/fr/fr.csv").using(new R).foreach[String]('line) { case (r: R, value: String) => r.processLine(value) }

}

object Ing extends App {

  run()

  def run(): Unit = {
    lazy val sft = SimpleFeatureTypes.createType("frz3", "dtg:Date,jsonrowid:String,registration:String,lat:Double,lon:Double,trackingdegrees:Integer,altitude:Integer,speedkt:Integer,squawk:String,radarid:String,planetype:String,tailname:String,datatimestamp:Long,departure:String,arrival:String,callsign:String:index=full,possibleonground:Boolean,vertspeed:Integer,callsign2:String,unknown1:Integer,*geom:Point:srid=4326:index=full:index-value=true")
    val retds = DataStoreFinder.getDataStore(
      Map(
        "instanceId" -> "ds",
        "zookeepers"   -> "zoo1,zoo2,zoo3",
        "tableName" -> "frz3kryo",
        "user"->"root",
        "password"->"secret"))

    if(retds.getSchema("frz3") == null) {
      retds.createSchema(sft)
    }

     val conf = new Configuration()

    // setup ingest
    val mode = Hdfs(strict = true, conf)

    val arguments = Mode.putMode(mode, Args(args))
    val job = new Ing(arguments)
    val flow = job.buildFlow

    //block until job is completed.
    flow.complete()
   }

}