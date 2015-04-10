/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.feature.serialization.kryo

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.feature.serialization.{AbstractReader, SimpleFeatureDecodingsCache}

/** Concrete [[SimpleFeatureDecodingsCache]] for Kryo. */
object KryoSimpleFeatureDecodingsCache extends SimpleFeatureDecodingsCache[Input] {

  override protected val datumReadersFactory: () => AbstractReader[Input] = () => new KryoReader()
}
