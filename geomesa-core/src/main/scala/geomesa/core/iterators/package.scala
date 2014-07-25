package geomesa.core

import collection.JavaConversions._
import org.apache.accumulo.core.client.IteratorSetting
import org.opengis.feature.simple.SimpleFeatureType


package object iterators {
  val FEATURE_ENCODING   = "geomesa.feature.encoding"
  val USER_DATA = ".userdata."

  implicit class RichIteratorSetting(cfg: IteratorSetting) {
    /**
     *  Copy UserData entries taken from a SimpleFeatureType into an IteratorSetting for later transfer back into
     *  a SimpleFeatureType
     *
     *  This works around the fact that SimpleFeatureTypes.encodeType ignores the UserData
     *
     */
    def encodeUserData(userData: java.util.Map[AnyRef,AnyRef], keyPrefix: String)  {
      val fullPrefix = keyPrefix + USER_DATA
      userData.foreach { case (k, v) => cfg.addOption(fullPrefix + k.toString, v.toString)}
    }
  }

  implicit class RichIteratorSimpleFeatureType(sft: SimpleFeatureType) {
    /**
     *  Copy UserData entries taken from an IteratorSetting/Options back into
     *  a SimpleFeatureType
     *
     *  This works around the fact that SimpleFeatureTypes.encodeType ignores the UserData
     *
     */
    def decodeUserData(options: java.util.Map[String,String], keyPrefix:String)  {
      val fullPrefix = keyPrefix + USER_DATA
      options
        .filter {  case (k, _) => k.startsWith(fullPrefix) }
        .foreach { case (k, v) => sft.getUserData.put(k.stripPrefix(fullPrefix), v) }
    }
  }
}
