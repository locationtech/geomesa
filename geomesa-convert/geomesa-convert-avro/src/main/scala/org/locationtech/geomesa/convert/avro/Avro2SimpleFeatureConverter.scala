/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert.{Field, SimpleFeatureConverter, SimpleFeatureConverterFactory, ToSimpleFeatureConverter}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class Avro2SimpleFeatureConverterBuilder extends SimpleFeatureConverterFactory[Array[Byte]] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "avro")

  override def buildConverter(targetSFT: SimpleFeatureType, conf: Config): SimpleFeatureConverter[Array[Byte]] = {
    val avroSchemaPath = conf.getString("schema")
    val avroSchema = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(avroSchemaPath))
    val reader = new GenericDatumReader[GenericRecord](avroSchema)
    val fields = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))

    new Avro2SimpleFeatureConverter(avroSchema, reader, targetSFT, fields, idBuilder)
  }

}

class Avro2SimpleFeatureConverter(avroSchema: Schema,
                                  reader: GenericDatumReader[GenericRecord],
                                  val targetSFT: SimpleFeatureType,
                                  val inputFields: IndexedSeq[Field],
                                  val idBuilder: Expr)
  extends ToSimpleFeatureConverter[Array[Byte]] {

  var decoder: BinaryDecoder = null
  var recordReuse: GenericRecord = null

  override def fromInputType(bytes: Array[Byte]): Array[Any] = {
    decoder = DecoderFactory.get.binaryDecoder(bytes, decoder)
    Array(bytes, reader.read(recordReuse, decoder))
  }

}
