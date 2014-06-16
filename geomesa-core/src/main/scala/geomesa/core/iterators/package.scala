package geomesa.core

import org.apache.accumulo.core.client.IteratorSetting
import collection.JavaConversions._
import org.opengis.feature.simple.SimpleFeatureType

package object iterators {
  val FEATURE_ENCODING   = "geomesa.feature.encoding"
  val USER_DATA = ".userdata."

  implicit class RichIteratorSetting(cfg: IteratorSetting) {
    /**
     *  Copy UserData entries taken from a SimpleFeatureType into an IteratorSetting for later transfer back into
     *  a SimpleFeatureType
     *
     *  This works around the fact that DataUtilities.encodeType ignores the UserData
     *
     */
    def encodeUserData(userData: java.util.Map[AnyRef,AnyRef], keyPrefix: String)  {
      val fullPrefix = keyPrefix+USER_DATA
      val userDataMap = userData.map { case (k, v) => (fullPrefix + k.toString, v.toString) }
      cfg.addOptions(userDataMap)
    }
  }
  implicit class RichIteratorSimpleFeatureType(sft: SimpleFeatureType) {
    /**
     *  Copy UserData entries taken from an IteratorSetting/Options back into
     *  a SimpleFeatureType
     *
     *  This works around the fact that DataUtilities.encodeType ignores the UserData
     *
     */
    def decodeUserData(options: java.util.Map[String,String], keyPrefix:String)  {
      val fullPrefix=keyPrefix+USER_DATA
      val userDataMap =
        options
          .filter {  case (k, _) => k.startsWith(fullPrefix) }
          .map {  case (k, v) => (k.stripPrefix(fullPrefix), v) }
      sft.getUserData.putAll(userDataMap)
    }
  }
}
