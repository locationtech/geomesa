package geomesa.tools

import java.io.File
import geomesa.core.data.AccumuloDataStore
import geomesa.core.index.Constants
import geomesa.feature.AvroSimpleFeatureFactory
import org.geotools.data.{DataStoreFinder, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


class GeoJsonIngest(config: Config, dsConfig: Map[String, _]) {
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