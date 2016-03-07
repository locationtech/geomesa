package org.locationtech.geomesa.dynamo.core

import org.opengis.feature.simple.SimpleFeatureType

trait DynamoContentState {

  val sft: SimpleFeatureType
}
