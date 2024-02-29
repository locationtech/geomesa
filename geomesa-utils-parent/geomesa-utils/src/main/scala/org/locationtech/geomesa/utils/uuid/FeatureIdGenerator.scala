package org.locationtech.geomesa.utils.uuid

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Trait for generating feature ids based on attributes of a simple feature
 */
trait FeatureIdGenerator {

  /**
   * Create a unique feature ID
   */
  def createId(sft: SimpleFeatureType, sf: SimpleFeature): String
}
