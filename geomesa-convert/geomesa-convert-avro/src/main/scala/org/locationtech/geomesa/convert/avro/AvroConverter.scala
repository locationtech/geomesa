/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroConverter._
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CopyingInputStream

import java.io.{ByteArrayOutputStream, InputStream}

class AvroConverter(sft: SimpleFeatureType, config: AvroConfig, fields: Seq[BasicField], options: BasicOptions)
    extends AbstractConverter[GenericRecord, AvroConfig, BasicField, BasicOptions](sft, config, fields, options) {

  private val schema = config.schema match {
    case SchemaEmbedded => None
    case SchemaString(s) => Some(new Parser().parse(s))
    case SchemaFile(s) =>
      val loader = Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader)
      val res = Option(loader.getResourceAsStream(s)).orElse(Option(getClass.getResourceAsStream(s))).getOrElse {
        throw new IllegalArgumentException(s"Could not load schema resource at $s")
      }
      Some(new Parser().parse(res))
  }

  // if required, set the raw bytes in the result array
  private val requiresBytes = {
    val expressions = config.idField.toSeq ++ fields.flatMap(_.transforms) ++ config.userData.values
    Expression.flatten(expressions).contains(Column(0))
  }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[GenericRecord] = {
    schema match {
      case Some(s) if requiresBytes => new GenericRecordBytesIterator(new CopyingInputStream(is), s, ec)
      case Some(s)                  => new GenericRecordIterator(is, s, ec)
      case None    if requiresBytes => new FileStreamBytesIterator(is, ec)
      case None                     => new FileStreamIterator(is, ec)
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
    schema.getType match {
      case Schema.Type.RECORD =>
        val fields = new java.util.ArrayList[Schema.Field](schema.getFields.size() + 1)
        schema.getFields.asScala.foreach { field =>
          fields.add(new Schema.Field(field.name, field.schema, field.doc, field.defaultVal()))
        }
        fields.add(new Schema.Field(BytesField, Schema.create(Schema.Type.BYTES), "raw bytes", ""))

        val updated = Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError)
        updated.setFields(fields)
        updated

      case Schema.Type.UNION =>
        Schema.createUnion(schema.getTypes.asScala.map(s => addBytes(s)).toSeq: _*)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
        Schema.createUnion(schema.getTypes.asScala.map(addBytes): _*)
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        Schema.createUnion(schema.getTypes.asScala.map(addBytes): _*)
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        Schema.createUnion(schema.getTypes.asScala.map(addBytes): _*)
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Schema.createUnion(schema.getTypes.asScala.map(addBytes): _*)
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Schema.createUnion(schema.getTypes.asScala.map(addBytes): _*)
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)

      case _ =>
        throw new NotImplementedError(
          s"Raw Avro bytes (i.e. $$0) is not implemented for schema type ${schema.getType}")
    }
  }

  case class AvroConfig(
      `type`: String,
      schema: SchemaConfig,
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig

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
    * @param ec evaluation context
    */
  class GenericRecordIterator private [AvroConverter] (is: InputStream, schema: Schema, ec: EvaluationContext)
      extends CloseableIterator[GenericRecord] {

    private val reader = new GenericDatumReader[GenericRecord](schema)
    private val decoder = DecoderFactory.get.binaryDecoder(is, null)
    private var record: GenericRecord = _

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): GenericRecord = {
      ec.line += 1
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
    * @param ec evaluation context
    */
  class GenericRecordBytesIterator private [AvroConverter] (is: CopyingInputStream, schema: Schema, ec: EvaluationContext)
      extends CloseableIterator[GenericRecord] {

    private val reader = new GenericDatumReader[GenericRecord](schema, addBytes(schema))
    private val decoder = DecoderFactory.get.binaryDecoder(is, null)
    private var record: GenericRecord = _

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): GenericRecord = {
      ec.line += 1
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
    * @param ec evaluation context
    */
  class FileStreamIterator private [AvroConverter] (is: InputStream, ec: EvaluationContext)
      extends CloseableIterator[GenericRecord] {

    private val stream = new DataFileStream(is, new GenericDatumReader[GenericRecord]())
    private var record: GenericRecord = _

    override def hasNext: Boolean = stream.hasNext

    override def next(): GenericRecord = {
      ec.line += 1
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
    * @param ec evaluation context
    */
  class FileStreamBytesIterator private [AvroConverter] (is: InputStream, ec: EvaluationContext)
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
      ec.line += 1
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
