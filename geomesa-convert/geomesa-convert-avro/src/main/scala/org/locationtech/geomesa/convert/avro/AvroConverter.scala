/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.avro.message.SchemaStore
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroConverter._
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig, ConverterName}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CopyingInputStream

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

class AvroConverter(sft: SimpleFeatureType, config: AvroConfig, fields: Seq[BasicField], options: BasicOptions)
    extends AbstractConverter[GenericRecord, AvroConfig, BasicField, BasicOptions](sft, config, fields, options) {

  // if required, set the raw bytes in the result array
  private val requiresBytes = {
    val expressions = config.idField.toSeq ++ fields.flatMap(_.transforms) ++ config.userData.values
    Expression.flatten(expressions).contains(Column(0))
  }

  private val schema: Option[Either[Schema, SchemaStore]] = config.schema match {
    case SchemaEmbedded => None
    case SchemaString(s) => Some(Left(new Parser().parse(s)))
    case SchemaFile(s) => Some(Left(new Parser().parse(loadSchemaFile(s))))
    case SchemaFiles(seq) =>
      val store = new SchemaStore.Cache()
      seq.foreach(s => store.addSchema(new Parser().parse(loadSchemaFile(s))))
      Some(Right(store))
  }

  private val iteratorPool: GenericObjectPool[GenericRecordIterator] = {
    val factory = new BasePooledObjectFactory[GenericRecordIterator] {
      override def create(): GenericRecordIterator = createNewIterator()
      override def wrap(obj: GenericRecordIterator): PooledObject[GenericRecordIterator] = new DefaultPooledObject(obj)
    }
    val config = new GenericObjectPoolConfig[GenericRecordIterator]()
    config.setMaxTotal(-1) // unlimited size
    new GenericObjectPool(factory, config)
  }

  private def createNewIterator(): GenericRecordIterator = {
    if (requiresBytes) {
      schema match {
        case None               => new FileStreamBytesIterator(iteratorPool)
        case Some(Left(s))      => new KnownSchemaBytesIterator(iteratorPool, s)
        case Some(Right(store)) => new SingleObjectBytesIterator(iteratorPool, store)
      }
    } else {
      schema match {
        case None               => new FileStreamIterator(iteratorPool)
        case Some(Left(s))      => new KnownSchemaIterator(iteratorPool, s)
        case Some(Right(store)) => new SingleObjectIterator(iteratorPool, store)
      }
    }
  }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[GenericRecord] = {
    val iter = iteratorPool.borrowObject()
    iter.setInstance(is, ec)
    iter
  }

  override protected def values(parsed: CloseableIterator[GenericRecord], ec: EvaluationContext): CloseableIterator[Array[Any]] = {
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

      case _ =>
        throw new UnsupportedOperationException(
          s"Raw Avro bytes (i.e. $$0) is not implemented for schema type ${schema.getType}")
    }
  }

  private def loadSchemaFile(file: String): InputStream = {
    val loader = Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader)
    Option(loader.getResourceAsStream(file)).orElse(Option(getClass.getResourceAsStream(file))).getOrElse {
      throw new IllegalArgumentException(s"Could not load schema resource at $file")
    }
  }

  case class AvroConfig(
      `type`: String,
      converterName: Option[String],
      schema: SchemaConfig,
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig with ConverterName

  sealed trait SchemaConfig

  case class SchemaString(schema: String) extends SchemaConfig
  case class SchemaFile(file: String) extends SchemaConfig
  case class SchemaFiles(files: Seq[String]) extends SchemaConfig
  case object SchemaEmbedded extends SchemaConfig {
    val name: String = "embedded"
  }

  private abstract class GenericRecordIterator(pool: GenericObjectPool[GenericRecordIterator])
      extends CloseableIterator[GenericRecord] {

    private val closed = new AtomicBoolean(false)

    def setInstance(is: InputStream, ec: EvaluationContext): Unit

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        pool.returnObject(this)
      }
    }
  }

  /**
   * Reads avro records using a pre-defined schema
   *
   * @param pool iterator pool
   * @param schema write schema
   * @param readSchema read schema
   */
  private class KnownSchemaIterator(pool: GenericObjectPool[GenericRecordIterator], schema: Schema, readSchema: Option[Schema] = None)
      extends GenericRecordIterator(pool) {

    private val reader = new GenericDatumReader[GenericRecord](schema, readSchema.getOrElse(schema))
    protected var decoder: BinaryDecoder = _
    private var is: InputStream = _
    private var record: GenericRecord = _
    private var ec: EvaluationContext = _

    override def setInstance(is: InputStream, ec: EvaluationContext): Unit = {
      this.is = is
      this.ec = ec
      this.decoder = DecoderFactory.get.binaryDecoder(is, decoder)
      this.record = null
    }

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): GenericRecord = {
      ec.line += 1
      record = reader.read(record, decoder)
      record
    }

    override def close(): Unit = {
      is.close()
      super.close()
    }
  }

  /**
    * Reads avro records using a pre-defined schema, setting the bytes for each record in a
    * special `__bytes__` field
    *
    * @param schema schema
    */
  private class KnownSchemaBytesIterator(pool: GenericObjectPool[GenericRecordIterator], schema: Schema)
      extends KnownSchemaIterator(pool, schema, Some(addBytes(schema))) {

    private var copier: CopyingInputStream = _

    override def setInstance(is: InputStream, ec: EvaluationContext): Unit = {
      copier = new CopyingInputStream(is)
      super.setInstance(copier, ec)
    }

    override def next(): GenericRecord = {
      val record = super.next()
      // parse out the bytes read and set them in the record
      // check to see if the decoder buffered some bytes that weren't actually used
      val buffered = decoder.inputStream().available()
      record.put(BytesField, copier.replay(copier.copied - buffered))
      record
    }
  }

  /**
   * Reads avro records encoded as 'single objects' - see https://avro.apache.org/docs/1.11.4/specification/#single-object-encoding
   *
   * @param schemas schemas
   */
  private class SingleObjectIterator(pool: GenericObjectPool[GenericRecordIterator], schemas: SchemaStore)
      extends GenericRecordIterator(pool) {

    // the schema fingerprint is stored as a long in little-endian order - and we don't read any other values from the buffer
    protected var buffer: ByteBuffer = ByteBuffer.allocate(1024).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    private val readers = scala.collection.mutable.Map.empty[Long, GenericDatumReader[GenericRecord]]
    private var decoder: BinaryDecoder = _
    private var record: GenericRecord = _
    private var ec: EvaluationContext = _
    private var error: Throwable = _
    private var hasRecord = false

    override def setInstance(is: InputStream, ec: EvaluationContext): Unit = {
     try {
       populateBuffer(is)
       val fingerprint = readFingerprint()
       val schema = schemas.findByFingerprint(fingerprint)
       if (schema == null) {
         throw new IllegalStateException(s"Schema not found for fingerprint: $fingerprint")
       }
       this.ec = ec
       this.decoder = DecoderFactory.get.binaryDecoder(buffer.array(), buffer.position(), buffer.remaining(), decoder)
       val reader = readers.getOrElseUpdate(fingerprint, newReader(schema))
       this.record = reader.read(record, decoder)
       this.hasRecord = record != null
       this.error = null
     } catch {
       case NonFatal(e) => this.error = e
     } finally {
       is.close()
     }
    }

    override def hasNext: Boolean = {
      if (error != null) {
        throw error
      } else {
        hasRecord
      }
    }

    override def next(): GenericRecord = {
      ec.line += 1
      hasRecord = false
      record
    }

    protected def newReader(schema: Schema): GenericDatumReader[GenericRecord] =
      new GenericDatumReader[GenericRecord](schema, schema)

    // read all bytes from the input stream into the buffer, expanding as necessary
    private def populateBuffer(is: InputStream): Unit = {
      buffer.clear()
      var read = 0
      var chunk = 0
      while ({ chunk = is.read(buffer.array(), buffer.position(), buffer.remaining()); chunk != -1 }) {
        read += chunk
        buffer.position(read)
        if (buffer.remaining() == 0) {
          // expand the buffer by doubling its capacity
          val newBuffer = ByteBuffer.allocate(buffer.capacity() * 2).order(java.nio.ByteOrder.LITTLE_ENDIAN)
          buffer.flip()
          newBuffer.put(buffer)
          buffer = newBuffer
        }
      }
      buffer.flip()
    }

    private def readFingerprint(): Long = {
      // validate buffer has at least 10 bytes (2-byte header + 8-byte fingerprint)
      if (buffer.remaining() < 10) {
        throw new IllegalArgumentException(
          s"Invalid Avro single-object encoding: expected at least 10 bytes, got ${buffer.remaining()}")
      }

      // check for Avro single-object magic bytes (C3 01)
      val byte0 = buffer.get() & 0xFF
      val byte1 = buffer.get() & 0xFF
      if (byte0 != 0xC3 || byte1 != 0x01) {
        throw new IllegalArgumentException(
          f"Invalid Avro single-object encoding: expected magic bytes C3 01, got $byte0%02X $byte1%02X")
      }

      // read the 8-byte fingerprint in little-endian order to match BinaryMessageEncoder
      buffer.getLong()
    }
  }

  /**
   * Reads avro records encoded as 'single objects', setting the bytes for each record in a special `__bytes__` field
   *
   * @param schemas schemas
   */
  private class SingleObjectBytesIterator(pool: GenericObjectPool[GenericRecordIterator], schemas: SchemaStore)
      extends SingleObjectIterator(pool, schemas) {

    override def next(): GenericRecord = {
      val rec = super.next()
      if (rec != null) {
        val bytes = Array.ofDim[Byte](buffer.limit())
        System.arraycopy(buffer.array(), 0, bytes, 0, buffer.limit())
        rec.put(BytesField, bytes)
      }
      rec
    }

    override protected def newReader(schema: Schema): GenericDatumReader[GenericRecord] =
      new GenericDatumReader[GenericRecord](schema, addBytes(schema))
  }

  /**
    * Reads avro records from an avro file, with the schema embedded
    */
  private class FileStreamIterator(pool: GenericObjectPool[GenericRecordIterator]) extends GenericRecordIterator(pool) {

    private var stream: DataFileStream[GenericRecord] = _
    private var record: GenericRecord = _
    private var ec: EvaluationContext = _

    override def setInstance(is: InputStream, ec: EvaluationContext): Unit = {
      this.ec = ec
      this.stream = new DataFileStream(is, new GenericDatumReader[GenericRecord]())
      this.record = null
    }

    override def hasNext: Boolean = stream.hasNext

    override def next(): GenericRecord = {
      ec.line += 1
      record = stream.next(record)
      record
    }

    override def close(): Unit = {
      stream.close()
      super.close()
    }
  }

  /**
    * Reads avro records from an avro file, with the schema embedded, setting the bytes for
    * each record in a special `__bytes__` field
    */
  private class FileStreamBytesIterator(pool: GenericObjectPool[GenericRecordIterator]) extends GenericRecordIterator(pool) {

    private val reader = new GenericDatumReader[GenericRecord]()
    private var stream: DataFileStream[GenericRecord] = _
    private var record: GenericRecord = _
    private var ec: EvaluationContext = _

    // re-serialize each record to get the raw bytes
    private val out = new ByteArrayOutputStream()
    private var writer: GenericDatumWriter[GenericRecord] = _
    private var encoder: BinaryEncoder = _

    override def setInstance(is: InputStream, ec: EvaluationContext): Unit = {
      this.ec = ec
      this.stream = new DataFileStream(is, reader)
      this.record = null

      reader.setExpected(addBytes(reader.getSchema))

      // we can't tell which bytes correspond to which feature (due to buffering). if we could access the
      // underlying avro encoder we could figure it out, but it is not exposed through DataFileStream. instead,
      // re-serialize each record to get the raw bytes
      this.writer = new GenericDatumWriter[GenericRecord](stream.getSchema)
      this.encoder = EncoderFactory.get.binaryEncoder(out, encoder)
    }

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

    override def close(): Unit = {
      stream.close()
      super.close()
    }
  }
}
