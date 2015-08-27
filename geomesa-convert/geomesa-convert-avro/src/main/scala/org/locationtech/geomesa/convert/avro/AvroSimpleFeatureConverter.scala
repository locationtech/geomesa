/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert.{Field, SimpleFeatureConverter, SimpleFeatureConverterFactory, ToSimpleFeatureConverter}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class AvroSimpleFeatureConverterFactory extends SimpleFeatureConverterFactory[Array[Byte]] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "avro")

  override def buildConverter(targetSFT: SimpleFeatureType, conf: Config): SimpleFeatureConverter[Array[Byte]] = {
    val avroSchemaPath = conf.getString("schema")
    val avroSchema = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(avroSchemaPath))
    val reader = new GenericDatumReader[GenericRecord](avroSchema)
    val fields = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))

    new AvroSimpleFeatureConverter(avroSchema, reader, targetSFT, fields, idBuilder)
  }

}

class AvroSimpleFeatureConverter(avroSchema: Schema,
                                  reader: GenericDatumReader[GenericRecord],
                                  val targetSFT: SimpleFeatureType,
                                  val inputFields: IndexedSeq[Field],
                                  val idBuilder: Expr)
  extends ToSimpleFeatureConverter[Array[Byte]] {

  var decoder: BinaryDecoder = null
  var recordReuse: GenericRecord = null

  override def fromInputType(bytes: Array[Byte]): Seq[Array[Any]] = {
    decoder = DecoderFactory.get.binaryDecoder(bytes, decoder)
    Seq(Array(bytes, reader.read(recordReuse, decoder)))
  }

}
