/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import java.nio.ByteBuffer

import com.vividsolutions.jts.io.InStream
import org.apache.avro.io.Decoder
import org.locationtech.geomesa.features.avro.AvroSimpleFeature
import org.locationtech.geomesa.utils.text.WKBUtils

/**
 * AvroSimpleFeature version 2 changes serialization of Geometry types from
 * WKT (Well Known Text) to WKB (Well Known Binary)
 */
object Version2Deserializer extends ASFDeserializer {

  override def setGeometry(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    val bb = in.readBytes(null)
    val bytes = new Array[Byte](bb.remaining)
    bb.get(bytes)
    val geom = WKBUtils.read(bytes)
    sf.setAttributeNoConvert(field, geom)
  }

  class BBInStream(bb: ByteBuffer) extends InStream {
    override def read(buf: Array[Byte]): Unit = bb.get(buf)
  }

  override def consumeGeometry(in: Decoder) = in.skipBytes()

}

