/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro.registry

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroConverter
import org.locationtech.geomesa.convert.avro.registry.AvroSchemaRegistryConverter.{AvroSchemaRegistryConfig, GenericRecordSchemaRegistryBytesIterator, GenericRecordSchemaRegistryIterator}
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig, ConverterName}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CopyingInputStream

import java.io.InputStream
import java.nio.ByteBuffer

class AvroSchemaRegistryConverter(
    sft: SimpleFeatureType,
    config: AvroSchemaRegistryConfig,
    fields: Seq[BasicField],
    options: BasicOptions
  ) extends AbstractConverter[GenericRecord, AvroSchemaRegistryConfig, BasicField, BasicOptions](
    sft, config, fields, options) {

  private val schemaRegistryClient = new CachedSchemaRegistryClient(config.schemaRegistry, 100)

  // if required, set the raw bytes in the result array
  private val requiresBytes = {
    val expressions = config.idField.toSeq ++ fields.flatMap(_.transforms) ++ config.userData.values
    Expression.flatten(expressions).contains(Column(0))
  }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[GenericRecord] = {
    if (requiresBytes) {
      new GenericRecordSchemaRegistryBytesIterator(new CopyingInputStream(is), ec, schemaRegistryClient)
    } else {
      new GenericRecordSchemaRegistryIterator(is, ec, schemaRegistryClient)
    }
  }

  override protected def values(
      parsed: CloseableIterator[GenericRecord],
      ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    val array = Array.ofDim[Any](2)
    if (requiresBytes) {
      parsed.map { record => array(0) = record.get(AvroConverter.BytesField); array(1) = record; array }
    } else {
      parsed.map { record => array(1) = record; array }
    }
  }
}

object AvroSchemaRegistryConverter {

  private val MagicByteLength = 1
  private val SchemaIdLength = 4

  case class AvroSchemaRegistryConfig(
      `type`: String,
      converterName: Option[String],
      schemaRegistry: String,
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig with ConverterName

  /**
   * Reads avro records using a cached confluent-style schema registry
   *
   * @param is input stream
   * @param ec evaluation context
   * @param client schema registry lookup
   */
  private class GenericRecordSchemaRegistryIterator(is: InputStream, ec: EvaluationContext, client: SchemaRegistryClient)
      extends CloseableIterator[GenericRecord] with LazyLogging{

    protected val decoder: BinaryDecoder = DecoderFactory.get.binaryDecoder(is, null)
    private val readers = scala.collection.mutable.Map.empty[Int, GenericDatumReader[GenericRecord]]
    private val schemaIdBytes = ByteBuffer.wrap(Array.ofDim[Byte](SchemaIdLength))
    private var record: GenericRecord = _

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): GenericRecord = {
      ec.line += 1
      // Read confluent-style bytes
      decoder.skipFixed(MagicByteLength)
      decoder.readFixed(schemaIdBytes.array(), 0, SchemaIdLength)

      val id = schemaIdBytes.position(0).getInt()
      val reader = readers.getOrElseUpdate(id, loadReader(id))

      record = reader.read(record, decoder)
      record
    }

    override def close(): Unit = is.close()

    protected def loadReader(id: Int): GenericDatumReader[GenericRecord] =
      new GenericDatumReader[GenericRecord](client.getById(id))
  }

  /**
   * Reads avro records using a cached confluent-style schema registry, adding the raw serialized bytes to the record
   *
   * @param is input stream
   * @param ec evaluation context
   */
  private class GenericRecordSchemaRegistryBytesIterator(is: CopyingInputStream, ec: EvaluationContext, client: SchemaRegistryClient)
      extends GenericRecordSchemaRegistryIterator(is, ec, client) with LazyLogging{

    override def next(): GenericRecord = {
      val record = super.next()
      // parse out the bytes read and set them in the record
      // check to see if the decoder buffered some bytes that weren't actually used
      val buffered = decoder.inputStream().available()
      record.put(AvroConverter.BytesField, is.replay(is.copied - buffered))
      record
    }

    override protected def loadReader(id: Int): GenericDatumReader[GenericRecord] = {
      val schema = client.getById(id)
      new GenericDatumReader[GenericRecord](schema, AvroConverter.addBytes(schema))
    }
  }
}
