/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features

import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.opengis.feature.simple.SimpleFeature

trait HasEncodingOptions {
  def options: SerializationOptions
}

/**
 * Interface to encode SimpleFeatures with a configurable serialization format.
 *
 * A SimpleFeatureEncoder is bound to a given SimpleFeatureType since serialization
 * may depend upon the schema of the feature type.
 *
 * SimpleFeatureEncoder classes may not be thread safe and should generally be used
 * as instance variables for performance reasons.
 */
trait SimpleFeatureSerializer extends HasEncodingOptions {
  def serialize(feature: SimpleFeature): Array[Byte]
}

/**
 * Interface to read SimpleFeatures with a configurable serialization format.
 *
 * A SimpleFeatureDecoder is bound to a given SimpleFeatureType since serialization
 * may depend upon the schema of the feature type.
 *
 * SimpleFeatureDecoder classes may not be thread safe and should generally be used
 * as instance variables for performance reasons.
 */
trait SimpleFeatureDeserializer extends HasEncodingOptions {
  def deserialize(bytes: Array[Byte]): SimpleFeature
}
