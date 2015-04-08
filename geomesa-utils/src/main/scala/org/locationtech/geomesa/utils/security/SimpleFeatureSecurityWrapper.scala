package org.locationtech.geomesa.utils.security

import org.opengis.feature.simple.SimpleFeature

import scala.language.implicitConversions

/**
 * Wraps a [[SimpleFeature]] allowing visibility to be accessed and modified.
 * 
 * Created by mmatz on 4/3/15.
 */
class SimpleFeatureSecurityWrapper(val sf: SimpleFeature) {

  /**
   * Sets the visibility to the given ``visibility`` expression.
   *
   * @param visibility the visibility expression
   */
  def visibility_=(visibility: String): Unit = SecurityUtils.setFeatureVisibility(sf, visibility)

  /**
   * @return the set visibility or ``null`` if none
   */
  def visibility: String = SecurityUtils.getVisibility(sf)
}

/**
 * Implicit wrap and unwrap.
 */
object SimpleFeatureSecurityWrapper {
  implicit def wrap(sf: SimpleFeature): SimpleFeatureSecurityWrapper = new SimpleFeatureSecurityWrapper(sf)
  implicit def unwrap(wrapper: SimpleFeatureSecurityWrapper): SimpleFeature = wrapper.sf
}
