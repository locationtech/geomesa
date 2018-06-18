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
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.locationtech.geomesa.convert.avro.AvroConverter._
import org.locationtech.geomesa.convert.{Counter, EvaluationContext}
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
    case SchemaString(s) => Some(new Parser().parse(s))
    case SchemaFile(s)   => Some(new Parser().parse(getClass.getResourceAsStream(s)))
    case SchemaEmbedded  => None
  }

  override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    schema match {
      case None    => new FileStreamIterator(is, ec.counter)
      case Some(s) => new GenericDatumReaderIterator(is, s, ec.counter)
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
    * Reader for IPC stream, where schema is defined ahead of time and not included in the input
    *
    * @param is input
    * @param schema avro schema
    * @param counter counter
    */
  class GenericDatumReaderIterator private [AvroConverter](is: InputStream, schema: Schema, counter: Counter)
      extends GenericRecordIterator(schema, counter) {

    private val decoder = DecoderFactory.get.binaryDecoder(is, null)

    override def hasNext: Boolean = !decoder.isEnd
    override protected def readNext(record: GenericRecord): GenericRecord = reader.read(record, decoder)
    override def close(): Unit = is.close()
  }

  /**
    * Reader for Avro files, where the schema is embedded in the file
    *
    * @param is input
    * @param counter counter
    */
  class FileStreamIterator private [AvroConverter] (is: InputStream, counter: Counter)
      extends GenericRecordIterator(null, counter) {

    private val stream = new DataFileStream(is, reader)

    override def hasNext: Boolean = stream.hasNext
    override protected def readNext(record: GenericRecord): GenericRecord = stream.next(record)
    override def close(): Unit = stream.close()
  }

  /**
    * Base class for reading generic records and returning them for conversion
    *
    * @param schema schema, may be null
    * @param counter counter
    */
  abstract class GenericRecordIterator private [AvroConverter] (schema: Schema, counter: Counter)
      extends CloseableIterator[Array[Any]] {

    protected val reader = new GenericDatumReader[GenericRecord](schema)
    private val array = Array.ofDim[Any](2)
    private var record: GenericRecord = _

    protected def readNext(record: GenericRecord): GenericRecord

    override def next(): Array[Any] = {
      counter.incLineCount()
      record = readNext(record)
      array(1) = record
      array
    }
  }
}
