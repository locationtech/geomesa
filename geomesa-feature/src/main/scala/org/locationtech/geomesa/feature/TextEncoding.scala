package org.locationtech.geomesa.feature

import org.geotools.data.DataUtilities
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/** Utility methods for encoding and decoding [[org.opengis.feature.simple.SimpleFeature]]s as text.
  *
  * Created by mmatz on 4/2/15.
  */
object TextEncoding {

  private val visPrefix = "|Visibility ["
  private val visSuffix = "]"

  /**
   * @param input the [[String]] to which ``visibility`` will be added
   * @param visibility the visibility to add to ``input``
   * @return a [[String]] containing both ``input`` and ``visibility``
   */
  def addVisibility(input: String, visibility: String): String = s"$input$visPrefix$visibility$visSuffix"

  /**
   * @param input a [[String]] containing a value and a visiblity as produced by ``addVisibility``
   * @return a tuple where the first element is ``input`` with out the visibility and the second is the visibility
   */
  def splitVisibility(input: String): (String, String) = {
    val index = input.indexOf(visPrefix)

    if (index < 0 || !input.endsWith(visSuffix)) {
      throw new IllegalArgumentException(s"Visibility not found in '$input'")
    }

    if (input.indexOf(visPrefix, index + 1) >= 0) {
      throw new IllegalArgumentException(s"Multiple visibilities found in '$input'")
    }

    val remaining = input.substring(0, index)
    val vis = input.substring(index + visPrefix.length, input.length - visSuffix.length)

    (remaining, vis)
  }
}

/**
 * This could be done more cleanly, but the object pool infrastructure already
 * existed, so it was quickest, easiest simply to abuse it.
 */
object ThreadSafeDataUtilities {
  private[this] val dataUtilitiesPool = ObjectPoolFactory(new Object, 1)

  def encodeFeature(feature:SimpleFeature): String = dataUtilitiesPool.withResource {
    _ => DataUtilities.encodeFeature(feature)
  }

  def createFeature(simpleFeatureType:SimpleFeatureType, featureString:String): SimpleFeature =
    dataUtilitiesPool.withResource {
      _ => DataUtilities.createFeature(simpleFeatureType, featureString)
    }
}
