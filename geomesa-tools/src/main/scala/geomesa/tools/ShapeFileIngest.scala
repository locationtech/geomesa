package geomesa.tools

import java.net.URL
import geomesa.core.index.Constants
import org.geotools.data.shapefile.ShapefileDataStore

class ShapeFileIngest(config: Config, dsConfig: Map[String, _]) {
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

  //lazy val sft = SimpleFeatureTypes.createType(typeName, sftSpec)


}
