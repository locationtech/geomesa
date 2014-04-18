/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.data

import geomesa.utils.text.ObjectPoolFactory
import org.apache.accumulo.core.data.Value
import org.geotools.data.DataUtilities
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import com.google.common.cache.{CacheLoader, CacheBuilder, LoadingCache}
import geomesa.avro.scala.{AvroSimpleFeature, FeatureSpecificReader}
import java.util.concurrent.TimeUnit
import org.apache.avro.io.DecoderFactory
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}


/**
 * Responsible for collecting data-entries that share an identifier, and
 * when done, collapsing these rows into a single SimpleFeature.
 *
 * All encoding/decoding/serializing of features should be done through this
 * single class to allow future versions of serialization instead of scattering
 * knowledge of how the serialization is done through the geomesa-core codebase
 */

// TODO the FeatureSpecificReader may not be threadsafe...evaluate.
object SimpleFeatureEncoder {

  def encode(feature: SimpleFeature): Value = {
    val asf = AvroSimpleFeature(feature)
    val baos = new ByteArrayOutputStream()
    asf.write(baos)
    new Value(baos.toByteArray)
  }

  def decode(simpleFeatureType: SimpleFeatureType, featureValue: Value) = {
    val bais = new ByteArrayInputStream(featureValue.get())
    val decoder = DecoderFactory.get().binaryDecoder(bais, null)
    readerCache.get(simpleFeatureType).read(null, decoder)
  }

  def extractFeatureId(value: Value) = FeatureSpecificReader.extractId(new ByteArrayInputStream(value.get()))

  val readerCache: LoadingCache[SimpleFeatureType, FeatureSpecificReader] =
    AvroSimpleFeature.loadingCacheBuilder {
      sft =>
        FeatureSpecificReader(sft)
    }
}

