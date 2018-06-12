/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.io.InputStream

import com.typesafe.config.Config
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroConverter.AvroConfig
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType

class AvroConverter(targetSft: SimpleFeatureType,
                    config: AvroConfig,
                    fields: Seq[BasicField],
                    options: BasicOptions)
    extends AbstractConverter(targetSft, config, fields, options) {

  private val schema = config.schema match {
    case Left(s)  => new Parser().parse(s)
    case Right(s) => new Parser().parse(getClass.getResourceAsStream(s))
  }

  private val reader = new GenericDatumReader[GenericRecord](schema)

  private var decoder: BinaryDecoder = _

  override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    new CloseableIterator[Array[Any]] {

      private var record: GenericRecord = _
      private val array = Array.ofDim[Any](2)
      decoder = DecoderFactory.get.binaryDecoder(is, decoder)

      override def hasNext: Boolean = !decoder.isEnd

      override def next(): Array[Any] = {
        record = reader.read(record, decoder)
        array(1) = record
        ec.counter.incLineCount()
        array
      }

      override def close(): Unit = {}
    }
  }
}

object AvroConverter {

  case class AvroConfig(`type`: String,
                        schema: Either[String, String], // left indicates string to parse, right indicates resource
                        idField: Option[Expression],
                        caches: Map[String, Config],
                        userData: Map[String, Expression]) extends ConverterConfig
}
