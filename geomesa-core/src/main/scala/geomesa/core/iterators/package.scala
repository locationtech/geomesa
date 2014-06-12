package geomesa.core

import org.apache.accumulo.core.client.IteratorSetting
import collection.JavaConverters._
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
      val userDataMap = for {(k,v) <- userData.asScala} yield (fullPrefix + k.toString, v.toString)
      cfg.addOptions(userDataMap.asJava)
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
      val userDataMap = for {(k: String, v: String) <- options.asScala if k startsWith fullPrefix}
                                                 yield (k.stripPrefix(fullPrefix), v)
      sft.getUserData.putAll(userDataMap.asJava)
    }
  }
}
