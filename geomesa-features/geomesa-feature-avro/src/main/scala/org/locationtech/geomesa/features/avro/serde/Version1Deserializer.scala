/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.avro.io.Decoder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeature
import org.locationtech.geomesa.utils.text.WKTUtils

/**
 * Version 1 AvroSimpleFeature encodes fields as WKT (Well Known Text) in an Avro String
 */
object Version1Deserializer extends ASFDeserializer {

  override def setGeometry(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    val length = bb.remaining
    if (bytes.length < length) {
      bytes = Array.ofDim(length)
    }
    buffers.put((bb, bytes))
    bb.get(bytes, 0, length)
    sf.setAttributeNoConvert(field, WKTUtils.read(new String(bytes, 0, length, StandardCharsets.UTF_8)))
  }

  override def setGeometry(sf: ScalaSimpleFeature, field: Int, in:Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    val length = bb.remaining
    if (bytes.length < length) {
      bytes = Array.ofDim(length)
    }
    buffers.put((bb, bytes))
    bb.get(bytes, 0, length)
    sf.setAttributeNoConvert(field, WKTUtils.read(new String(bytes, 0, length, StandardCharsets.UTF_8)))
  }

  override def consumeGeometry(in: Decoder): Unit = in.skipBytes()
}
