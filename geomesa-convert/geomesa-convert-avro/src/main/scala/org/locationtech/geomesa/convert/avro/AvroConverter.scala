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
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.codehaus.jackson.node.TextNode
import org.locationtech.geomesa.convert.avro.AvroConverter._
import org.locationtech.geomesa.convert.{Counter, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CopyingInputStream
import org.opengis.feature.simple.SimpleFeatureType

class AvroConverter(sft: SimpleFeatureType, config: AvroConfig, fields: Seq[BasicField], options: BasicOptions)
    extends AbstractConverter[GenericRecord, AvroConfig, BasicField, BasicOptions](sft, config, fields, options) {

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

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[GenericRecord] = {
    schema match {
      case Some(s) if requiresBytes => new GenericRecordBytesIterator(new CopyingInputStream(is), s, ec.counter)
      case Some(s)                  => new GenericRecordIterator(is, s, ec.counter)
      case None    if requiresBytes => new FileStreamBytesIterator(is, ec.counter)
      case None                     => new FileStreamIterator(is, ec.counter)
    }
  }

  override protected def values(parsed: CloseableIterator[GenericRecord],
                                ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    val array = Array.ofDim[Any](2)
    if (requiresBytes) {
      parsed.map { record => array(0) = record.get(BytesField); array(1) = record; array }
    } else {
      parsed.map { record => array(1) = record; array }
    }
  }
}

object AvroConverter {

  import scala.collection.JavaConverters._

  val BytesField = "__bytes__"

  /**
    * Add a `__bytes__` field to the schema, for storing the raw bytes
    *
    * @param schema schema
    * @return
    */
  def addBytes(schema: Schema): Schema = {
    val fields = new java.util.ArrayList[Schema.Field](schema.getFields.size() + 1)
    schema.getFields.asScala.foreach { field =>
      fields.add(new Schema.Field(field.name, field.schema, field.doc, field.defaultValue()))
    }
    fields.add(new Schema.Field(BytesField, Schema.create(Schema.Type.BYTES), "raw bytes", TextNode.valueOf("")))

    val updated = Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError)
    updated.setFields(fields)
    updated
  }

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
  class GenericRecordIterator private [AvroConverter] (is: InputStream, schema: Schema, counter: Counter)
      extends CloseableIterator[GenericRecord] {

    private val reader = new GenericDatumReader[GenericRecord](schema)
    private val decoder = DecoderFactory.get.binaryDecoder(is, null)
    private var record: GenericRecord = _

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): GenericRecord = {
      counter.incLineCount()
      record = reader.read(record, decoder)
      record
    }

    override def close(): Unit = is.close()
  }

  /**
    * Reads avro records using a pre-defined schema, setting the bytes for each record in a
    * special `__bytes__` field
    *
    * @param is input stream
    * @param schema schema
    * @param counter counter
    */
  class GenericRecordBytesIterator private [AvroConverter] (is: CopyingInputStream, schema: Schema, counter: Counter)
      extends CloseableIterator[GenericRecord] {

    private val reader = new GenericDatumReader[GenericRecord](schema, addBytes(schema))
    private val decoder = DecoderFactory.get.binaryDecoder(is, null)
    private var record: GenericRecord = _

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): GenericRecord = {
      counter.incLineCount()
      record = reader.read(record, decoder)
      // parse out the bytes read and set them in the record
      // check to see if the decoder buffered some bytes that weren't actually used
      val buffered = decoder.inputStream().available()
      record.put(BytesField, is.replay(is.copied - buffered))
      record
    }

    override def close(): Unit = is.close()
  }

  /**
    * Reads avro records from an avro file, with the schema embedded
    *
    * @param is input
    * @param counter counter
    */
  class FileStreamIterator private [AvroConverter] (is: InputStream, counter: Counter)
      extends CloseableIterator[GenericRecord] {

    private val stream = new DataFileStream(is, new GenericDatumReader[GenericRecord]())
    private var record: GenericRecord = _

    override def hasNext: Boolean = stream.hasNext

    override def next(): GenericRecord = {
      counter.incLineCount()
      record = stream.next(record)
      record
    }

    override def close(): Unit = stream.close()
  }

  /**
    * Reads avro records from an avro file, with the schema embedded, setting the bytes for
    * each record in a special `__bytes__` field
    *
    * @param is input
    * @param counter counter
    */
  class FileStreamBytesIterator private [AvroConverter] (is: InputStream, counter: Counter)
      extends CloseableIterator[GenericRecord] {

    private val reader = new GenericDatumReader[GenericRecord]()
    private val stream = new DataFileStream(is, reader)
    private var record: GenericRecord = _

    reader.setExpected(addBytes(reader.getSchema))

    // we can't tell which bytes correspond to which feature (due to buffering). if we could access the
    // underlying avro encoder we could figure it out, but it is not exposed through DataFileStream. instead,
    // re-serialize each record to get the raw bytes
    private val out = new ByteArrayOutputStream()
    private val writer = new GenericDatumWriter[GenericRecord](stream.getSchema)
    private val encoder = EncoderFactory.get.binaryEncoder(out, null)

    override def hasNext: Boolean = stream.hasNext

    override def next(): GenericRecord = {
      counter.incLineCount()
      record = stream.next(record)
      // regenerate the bytes read and set them in the record
      out.reset()
      writer.write(record, encoder)
      encoder.flush()
      record.put(BytesField, out.toByteArray)
      record
    }

    override def close(): Unit = stream.close()
  }
}
