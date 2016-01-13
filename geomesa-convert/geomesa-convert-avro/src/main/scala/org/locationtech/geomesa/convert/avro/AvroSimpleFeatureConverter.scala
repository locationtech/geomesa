/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.avro

import java.io.InputStream

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class AvroSimpleFeatureConverterFactory extends SimpleFeatureConverterFactory[Array[Byte]] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "avro")

  override def buildConverter(targetSFT: SimpleFeatureType, conf: Config): SimpleFeatureConverter[Array[Byte]] = {
    val avroSchema =
      if (conf.hasPath("schema-file")) {
        new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(conf.getString("schema-file")))
      } else {
        new org.apache.avro.Schema.Parser().parse(conf.getString("schema"))
      }

    val reader = new GenericDatumReader[GenericRecord](avroSchema)
    val fields = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))

    new AvroSimpleFeatureConverter(avroSchema, reader, targetSFT, fields, idBuilder, isValidating(conf))
  }

}

class AvroSimpleFeatureConverter(avroSchema: Schema,
                                  reader: GenericDatumReader[GenericRecord],
                                  val targetSFT: SimpleFeatureType,
                                  val inputFields: IndexedSeq[Field],
                                  val idBuilder: Expr,
                                  val validating: Boolean)
  extends ToSimpleFeatureConverter[Array[Byte]] {

  var decoder: BinaryDecoder = null
  var recordReuse: GenericRecord = null

  override def fromInputType(bytes: Array[Byte]): Seq[Array[Any]] = {
    decoder = DecoderFactory.get.binaryDecoder(bytes, decoder)
    Seq(Array(bytes, reader.read(recordReuse, decoder)))
  }

  override def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature] = {
    decoder = DecoderFactory.get.binaryDecoder(is, null)

    class FeatureItr extends Iterator[SimpleFeature] {
      private var cur: SimpleFeature = null

      override def hasNext: Boolean = {
        if (cur == null) {
          do { fetchNext() } while (cur == null && !decoder.isEnd)
          cur != null
        } else {
          true
        }
      }

      override def next(): SimpleFeature = {
        hasNext
        if (cur != null) {
          val ret = cur
          cur = null
          ret
        } else throw new NoSuchElementException
      }

      def fetchNext() = {
        if (!decoder.isEnd) {
          ec.counter.incLineCount()
          val rec = reader.read(null, decoder)
          try {
            cur = convert(Array[Any](null, rec), ec)
            if (cur != null) {
              ec.counter.incSuccess()
            } else {
              ec.counter.incFailure()
            }
          }
          catch {
            case e: Exception =>
              logger.warn(s"Failed to parse avro record  '${rec.toString}'", e)
              ec.counter.incFailure()
          }
        }
      }
    }

    new FeatureItr
  }

}
