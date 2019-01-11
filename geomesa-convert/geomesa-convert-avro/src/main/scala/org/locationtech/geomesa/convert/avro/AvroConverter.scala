/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.io.{ByteArrayOutputStream, InputStream}

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.locationtech.geomesa.convert.avro.AvroConverter.{GenericRecordIterator, _}
import org.locationtech.geomesa.convert.{Counter, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CopyingInputStream
import org.opengis.feature.simple.SimpleFeatureType

class AvroConverter(targetSft: SimpleFeatureType,
                    config: AvroConfig,
                    fields: Seq[BasicField],
                    options: BasicOptions)
    extends AbstractConverter(targetSft, config, fields, options) {

  private val schema = config.schema match {
    case SchemaString(s) => Some(new Parser().parse(s))
    case SchemaFile(s)   => Some(new Parser().parse(getClass.getResourceAsStream(s)))
    case SchemaEmbedded  => None
  }

  // if required, set the raw bytes in the result array
  private val requiresBytes = {
    val expressions = config.idField.toSeq ++ fields.flatMap(_.transforms) ++ config.userData.values
    Expression.flatten(expressions).contains(Column(0))
  }

  override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    schema match {
      case Some(s) if requiresBytes => new GenericRecordByteIterator(new CopyingInputStream(is), s, ec.counter)
      case Some(s)                  => new GenericRecordIterator(is, s, ec.counter)
      case None    if requiresBytes => new FileStreamByteIterator(is, ec.counter)
      case None                     => new FileStreamIterator(is, ec.counter)
    }
  }
}

object AvroConverter {

  case class AvroConfig(`type`: String,
                        schema: SchemaConfig,
                        idField: Option[Expression],
                        caches: Map[String, Config],
                        userData: Map[String, Expression]) extends ConverterConfig

  sealed trait SchemaConfig

  case class SchemaString(schema: String) extends SchemaConfig
  case class SchemaFile(file: String) extends SchemaConfig
  case object SchemaEmbedded extends SchemaConfig {
    val name: String = "embedded"
  }

  /**
    * Reads avro records using a pre-defined schema
    *
    * @param is input stream
    * @param schema schema
    * @param counter counter
    */
  class GenericRecordIterator private [avro] (is: InputStream, schema: Schema, counter: Counter)
      extends CloseableIterator[Array[Any]] {

    private val reader = new GenericDatumReader[GenericRecord](schema)
    protected val decoder: BinaryDecoder = DecoderFactory.get.binaryDecoder(is, null)

    private var record: GenericRecord = _
    private val array = Array.ofDim[Any](2)

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): Array[Any] = {
      counter.incLineCount()
      record = reader.read(record, decoder)
      array(1) = record
      array
    }

    override def close(): Unit = is.close()
  }

  /**
    * Reads avro records using a pre-defined schema, setting the bytes for each record in $0 of the result array
    *
    * @param is input stream
    * @param schema schema
    * @param counter counter
    */
  class GenericRecordByteIterator private [avro] (is: CopyingInputStream, schema: Schema, counter: Counter)
      extends GenericRecordIterator(is, schema, counter) {

    override def next(): Array[Any] = {
      val array = super.next()
      val read = is.copy.toByteArray
      is.copy.reset()
      // check to see if the decoder buffered some bytes that weren't actually used
      val buffered = decoder.inputStream().available()
      if (buffered > 0) {
        // only return the bytes that were actually used
        val size = read.length - buffered
        array(0) = Array.ofDim[Byte](size)
        System.arraycopy(read, 0, array(0), 0, size)
        // write the unused bytes back to our buffer for the next read
        is.copy.write(read, size, buffered)
      } else {
        array(0) = read
      }
      array
    }
  }

  /**
    * Reads avro records from an avro file, with the schema embedded
    *
    * @param is input
    * @param counter counter
    */
  class FileStreamIterator private [avro] (is: InputStream, counter: Counter) extends CloseableIterator[Array[Any]] {

    protected val stream = new DataFileStream(is, new GenericDatumReader[GenericRecord]())
    protected var record: GenericRecord = _

    private val array = Array.ofDim[Any](2)

    override def hasNext: Boolean = stream.hasNext

    override def next(): Array[Any] = {
      counter.incLineCount()
      record = stream.next(record)
      array(1) = record
      array
    }

    override def close(): Unit = stream.close()
  }

  /**
    * Reads avro records from an avro file, with the schema embedded, setting the bytes for each
    * record in $0 of the result array
    *
    * @param is input
    * @param counter counter
    */
  class FileStreamByteIterator private [avro] (is: InputStream, counter: Counter)
      extends FileStreamIterator(is, counter) {

    private val out = new ByteArrayOutputStream()
    private val writer = new GenericDatumWriter[GenericRecord](stream.getSchema)
    private val encoder = EncoderFactory.get.binaryEncoder(out, null)

    override def hasNext: Boolean = stream.hasNext

    override def next(): Array[Any] = {
      val array = super.next()
      out.reset()
      writer.write(record, encoder)
      encoder.flush()
      array(0) = out.toByteArray
      array
    }

    override def close(): Unit = stream.close()
  }
}
