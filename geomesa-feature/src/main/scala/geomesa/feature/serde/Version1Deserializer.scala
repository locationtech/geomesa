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

import geomesa.feature.AvroSimpleFeature
import geomesa.utils.text.WKTUtils
import org.apache.avro.io.Decoder

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
