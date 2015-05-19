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

// This is the RasterIndexSchema, for more details see IndexSchema and SchemaHelpers

package org.locationtech.geomesa.raster.index

import org.locationtech.geomesa.accumulo.index.KeyValuePair
import org.locationtech.geomesa.raster.data.Raster

case class RasterIndexSchema(encoder: RasterEntryEncoder,
                             decoder: RasterEntryDecoder) {

  def encode(raster: Raster, visibility: String = "") = encoder.encode(raster, visibility)
  def decode(entry: KeyValuePair): Raster = decoder.decode(entry)

}

object RasterIndexSchema {

  def apply(): RasterIndexSchema = {
    val keyEncoder = new RasterEntryEncoder()
    val indexEntryDecoder = new RasterEntryDecoder()
    RasterIndexSchema(keyEncoder, indexEntryDecoder)
  }

}