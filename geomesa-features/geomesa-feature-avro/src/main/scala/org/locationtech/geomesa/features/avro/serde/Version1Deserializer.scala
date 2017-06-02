/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import org.apache.avro.io.Decoder
import org.locationtech.geomesa.features.avro.AvroSimpleFeature
import org.locationtech.geomesa.utils.text.WKTUtils

/**
 * Version 1 AvroSimpleFeature encodes fields as WKT (Well Known Text) in an Avro String
 */
object Version1Deserializer extends ASFDeserializer {

  override def setGeometry(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    val geom = WKTUtils.read(in.readString())
    sf.setAttributeNoConvert(field, geom)
  }

  // For good measure use skipString() even though it uses skipBytes() underneath
  // in order to protect from internal Avro changes
  override def consumeGeometry(in: Decoder) = in.skipString()

}
