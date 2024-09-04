/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.features.kryo.serialization.IndexValueSerializer

/**
  * Serializer for attribute join indices
  */
@deprecated("Replaced with org.locationtech.geomesa.features.kryo.serialization.IndexValueSerializer")
object IndexValueEncoder extends IndexValueSerializer {

  /**
    * Encoder/decoder for index values. Allows customizable fields to be encoded. Not thread-safe.
    *
    * @param sft simple feature type
    */
  @deprecated
  class IndexValueEncoderImpl(sft: SimpleFeatureType) extends IndexValueSerializer.IndexValueEncoderImpl(sft)
}
