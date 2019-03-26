/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro.registry

import java.io.InputStream
import java.nio.ByteBuffer

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.locationtech.geomesa.convert.avro.registry.AvroSchemaRegistryConverter.{AvroSchemaRegistryConfig, GenericRecordSchemaRegistryIterator}
import org.locationtech.geomesa.convert.{Counter, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType

class AvroSchemaRegistryConverter(sft: SimpleFeatureType, config: AvroSchemaRegistryConfig, fields: Seq[BasicField], options: BasicOptions)
  extends AbstractConverter[GenericRecord, AvroSchemaRegistryConfig, BasicField, BasicOptions](sft, config, fields, options) {

  private val schemaRegistryClient = new CachedSchemaRegistryClient(config.schemaRegistry, 100)

  private val kafkaAvroDeserializer = new ThreadLocal[KafkaAvroDeserializer]() {
    override def initialValue(): KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  }

  // Create schema registry reader from URL string and create Avro reader cache
  private val schemaRegistryConfig: LoadingCache[Integer, GenericDatumReader[GenericRecord]] =
    getReaderCache(kafkaAvroDeserializer)

  // Create Avro reader cache to map schema ID to GenericDatumReader
  def getReaderCache(deserializer: ThreadLocal[KafkaAvroDeserializer]): LoadingCache[Integer, GenericDatumReader[GenericRecord]] = {
    Caffeine
      .newBuilder()
      .build(
        new CacheLoader[Integer, GenericDatumReader[GenericRecord]] {
          override def load(id: Integer): GenericDatumReader[GenericRecord] = {
            new GenericDatumReader[GenericRecord](deserializer.get.getById(id))
          }
        }
      )
  }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[GenericRecord] = {
    new GenericRecordSchemaRegistryIterator(is, schemaRegistryConfig, ec.counter)
  }

  override protected def values(parsed: CloseableIterator[GenericRecord],
                                ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    val array = Array.ofDim[Any](2)
    parsed.map { record => array(1) = record; array }
  }
}

object AvroSchemaRegistryConverter {

  private val MAGIC_BYTE_LENGTH = 1
  private val SCHEMA_ID_LENGTH = 4

  case class AvroSchemaRegistryConfig(`type`: String,
                        schemaRegistry: String,
                        idField: Option[Expression],
                        caches: Map[String, Config],
                        userData: Map[String, Expression]) extends ConverterConfig

  sealed trait SchemaConfig

  case class SchemaRegistry(url: String) extends SchemaConfig

  /**
    * Reads avro records using a cached confluent-style schema registry
    *
    * @param is input stream
    * @param readerCache GenericDatumReader cache
    * @param counter counter
    */
  class GenericRecordSchemaRegistryIterator private [AvroSchemaRegistryConverter] (is: InputStream,
                                                                     readerCache: LoadingCache[Integer, GenericDatumReader[GenericRecord]],
                                                                     counter: Counter)
    extends CloseableIterator[GenericRecord] with LazyLogging{

    private val decoder = DecoderFactory.get.binaryDecoder(is, null)
    private var record: GenericRecord = _

    override def hasNext: Boolean = !decoder.isEnd

    override def next(): GenericRecord = {
      counter.incLineCount()
      // Read confluent-style bytes
      decoder.skipFixed(MAGIC_BYTE_LENGTH)
      val bytes = new Array[Byte](SCHEMA_ID_LENGTH)
      decoder.readFixed(bytes, 0, SCHEMA_ID_LENGTH)

      val id = ByteBuffer.wrap(bytes).getInt()
      val reader = readerCache.get(id)

      record = reader.read(record, decoder)
      record
    }

    override def close(): Unit = is.close()
  }
}
