package geomesa.tools


import java.io.File
import java.net.{URL, URLDecoder}
import java.nio.charset.Charset
import java.util.Date
import com.google.common.hash.Hashing
import com.vividsolutions.jts.geom.Coordinate
import geomesa.core.data.AccumuloDataStore
import geomesa.core.index.Constants
import geomesa.feature.AvroSimpleFeatureFactory
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geojson.feature._
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.collection.mutable.HashMap
import scala.io.Source

class Ingest() {
  val user = sys.env.getOrElse("GEOMESA_USER", "admin")
  val password = sys.env.getOrElse("GEOMESA_PASSWORD", "admin")
  val instanceId = sys.env.getOrElse("GEOMESA_INSTANCEID", "instanceId")
  val zookeepers = sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "zoo1:2181,zoo2:2181,zoo3:2181")
  val auths = sys.env.getOrElse("GEOMESA_AUTHS", "")
  val visibilities = sys.env.getOrElse("GEOMESA_VISIBILITIES", "")

  def getAccumuloDataStoreConf(config: Config): HashMap[String, Any] = {
    val dsConfig = HashMap[String, Any]()
    dsConfig.put("instanceId", instanceId)
    dsConfig.put("zookeepers", zookeepers)
    dsConfig.put("user", user)
    dsConfig.put("password", password)
    dsConfig.put("auths", auths)
    dsConfig.put("visibilities", visibilities)
    dsConfig.put("tableName", config.table)
    dsConfig
  }

  def defineIngestJob(config: Config): Boolean = {
    val dsConfig = getAccumuloDataStoreConf(config)
    config.format match {
      case "CSV" | "TSV" =>
        config.method match {
          case "mapreduce" =>
            println("Success")
            true
          case "naive" =>
            new SVIngest(config, dsConfig)
            true
          case _ =>
            println("Error, no such ingest method for CSV or TSV found, no data ingested")
            false
        }
      case "GEOJSON" | "JSON" =>
        config.method match {
          case "naive" =>
            new GeoJsonIngest(config, dsConfig)
            true
          case _ =>
            println("Error, no such ingest method for GEOJSON or JSON found, no data ingested")
            false
        }
      case "GML" | "KML" =>
        config.method match {
          case "naive" =>
            true
          case _ =>
            println("Error, no such ingest method for GML or KML found, no data ingested")
            false
        }
      case "SHAPEFILE" | "SHP" =>
        config.method match {
          case "naive" =>
            true
          case _ =>
            println("Error, no such ingest method for Shapefiles found, no data ingested")
            false
        }
      case _ =>
        println(s"Error, format: \'${config.format}\'")
        false
    }
  }
}

class SVIngest(config: Config, dsConfig: HashMap[String, Any]) {

  import scala.collection.JavaConversions._
  import scala.reflect.runtime.universe._
  def getConverter[T](clazz: Class[T])(implicit ct: TypeTag[T]): String => AnyRef =
    typeOf[T] match {
      case t if t =:= typeOf[java.lang.Float]   => s => java.lang.Float.valueOf(s)
      case t if t =:= typeOf[java.lang.Integer] => s => java.lang.Integer.valueOf(s)
      case t if t =:= typeOf[java.lang.Long]    => s => java.lang.Long.valueOf(s)
      case t if t =:= typeOf[java.lang.Double]  => s => java.lang.Double.valueOf(s)
      case t if t =:= typeOf[java.lang.Boolean] => s => java.lang.Boolean.valueOf(s)
      case t if t =:= typeOf[String]            => s => s
      case t if t =:= typeOf[Date]              => s => new DateTime(s)
      case _                                    => identity
    }

  lazy val table            = config.table
  lazy val path             = config.file
  lazy val typeName         = config.typeName
  lazy val idFields         = config.idFields
  lazy val sftSpec          = URLDecoder.decode(config.spec, "UTF-8")
  lazy val latField         = config.latField
  lazy val lonField         = config.lonField
  lazy val dtgField         = config.dtField
  lazy val dtgFmt           = config.dtFormat
  lazy val dtgTargetField   = Constants.SF_PROPERTY_START_TIME // sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
  lazy val zookeepers       = dsConfig.get("zookeepers")
  lazy val user             = dsConfig.get("user")
  lazy val password         = dsConfig.get("password")
  lazy val auths            = dsConfig.get("auths")

  lazy val delim  = config.format match {
    case "TSV" => "\t"
    case "CSV" => ","
  }

  lazy val sft = {
    val ret = DataUtilities.createType(typeName, sftSpec)
    ret.getUserData.put(Constants.SF_PROPERTY_START_TIME, dtgTargetField)
    ret
  }

  val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
  lazy val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
  ds.createSchema(sft) // this will need to be removed eventually

  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory
  lazy val dtFormat = DateTimeFormat.forPattern(dtgFmt)

  // NOTE: we assume that geom and dtg are the last elements of the sft and are computed from the data
  lazy val converters =
    sft.getAttributeDescriptors
      .take(sft.getAttributeDescriptors.length - 2)
      .map { desc => getConverter(desc.getType.getBinding) }

  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = buildDtBuilder
  lazy val idBuilder = buildIDBuilder

  val fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
  Source.fromFile(path).getLines.foreach { line => parseFeature(line) }
  def parseFeature(line: String) = {
    try {
      val fields = line.toString.split(delim)
      builder.reset()
      val id = idBuilder(fields)

      builder.addAll(convertAttributes(fields))
      val feature = builder.buildFeature(id)

      val lat = feature.getAttribute(latField).asInstanceOf[Double]
      val lon = feature.getAttribute(lonField).asInstanceOf[Double]
      val geom = geomFactory.createPoint(new Coordinate(lon, lat))
      val dtg = dtBuilder(feature.getAttribute(dtgField))

      feature.setDefaultGeometry(geom)
      feature.setAttribute(dtgTargetField, dtg.toDate)
      val toWrite = fw.next()
      sft.getAttributeDescriptors.foreach { ad =>
        toWrite.setAttribute(ad.getName, feature.getAttribute(ad.getName))
      }
      toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
      toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      fw.write()
    } catch {
      case t: Throwable => Nil //beware this hides the arrayindex out of bounds exception
    }
  }

  def convertAttributes(attrs: Array[String]): Seq[AnyRef] =
    converters.zip(attrs).map { case (conv, attr) => conv.apply(attr) }

  def buildIDBuilder: (Array[String]) => String = {
    idFields match {
      case s if "HASH".equals(s) =>
        val hashFn = Hashing.md5()
        attrs => hashFn.newHasher().putString(attrs.mkString("|"), Charset.defaultCharset()).hash().toString

      case s: String =>
        val idSplit = idFields.split(",").map { f => sft.indexOf(f) }
        attrs => idSplit.map { idx => attrs(idx) }.mkString("_")
    }
  }

  def buildDtBuilder: (AnyRef) => DateTime =
    attributes.find(_.getLocalName == dtgField).map {
      case attr if attr.getType.getBinding.equals(classOf[java.lang.Long]) =>
        (obj: AnyRef) => new DateTime(obj.asInstanceOf[java.lang.Long])

      case attr if attr.getType.getBinding.equals(classOf[java.util.Date]) =>
        (obj: AnyRef) => obj match {
          case d: java.util.Date => new DateTime(d)
          case s: String         => dtFormat.parseDateTime(s)
        }

      case attr if attr.getType.getBinding.equals(classOf[java.lang.String]) =>
        (obj: AnyRef) => dtFormat.parseDateTime(obj.asInstanceOf[String])

    }.getOrElse(throw new RuntimeException("Cannot parse date"))
  fw.close()
}

class GeoJsonIngest(config: Config, dsConfig: HashMap[String, Any]) {
  import geomesa.utils.geotools.Conversions.RichSimpleFeatureIterator
  import scala.collection.JavaConversions._
  lazy val table          = config.table
  lazy val path             = new File(config.file)
  lazy val typeName         = config.typeName
  lazy val dtgTargetField   = Constants.SF_PROPERTY_START_TIME // sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
  lazy val zookeepers       = dsConfig.get("zookeepers")
  lazy val user             = dsConfig.get("user")
  lazy val password         = dsConfig.get("password")
  lazy val auths            = dsConfig.get("auths")


  val io = new FeatureJSON()
  val jsonIterator =  io.streamFeatureCollection(path)
  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory

  val sft = io.readFeatureCollectionSchema(path, false)

  val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
  lazy val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]

  ds.createSchema(sft)

  lazy val attributes = sft.getAttributeDescriptors

  class CloseableFeatureWriter {
    val fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
    def release(): Unit = { fw.close() }
  }

  val cfw = new CloseableFeatureWriter

  jsonIterator.asInstanceOf[RichSimpleFeatureIterator].foreach{ f => parseFeature(cfw.fw, f) }

  def parseFeature(fw: FeatureWriter[SimpleFeatureType, SimpleFeature], feature: SimpleFeature): Unit = {
    try {
      val sft = feature.getFeatureType
      val toWrite = fw.next()
      sft.getAttributeDescriptors.foreach { ad =>
        toWrite.setAttribute(ad.getName, feature.getAttribute(ad.getName))
      }
      toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(feature.getID)
      toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      fw.write()
    }catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}

class ShapeFileIngest(config: Config, dsConfig: HashMap[String, Any]) {
  lazy val table          = config.table
  lazy val path             = new URL(config.file)
  lazy val typeName         = config.typeName
  lazy val dtgTargetField   = Constants.SF_PROPERTY_START_TIME // sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
  lazy val zookeepers       = dsConfig.get("zookeepers")
  lazy val user             = dsConfig.get("user")
  lazy val password         = dsConfig.get("password")
  lazy val auths            = dsConfig.get("auths")

  val shapeStore = new ShapefileDataStore(path)
  val name = shapeStore.getTypeNames()(0)


}
