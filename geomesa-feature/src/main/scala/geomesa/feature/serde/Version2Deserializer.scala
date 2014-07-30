/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.feature.serde

import java.nio.ByteBuffer

import com.vividsolutions.jts.io.InStream
import geomesa.feature.AvroSimpleFeature
import geomesa.utils.text.WKBUtils
import org.apache.avro.io.Decoder

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

