/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{JsonProperties, Schema}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeoMesaAvroDateFormat, GeoMesaAvroDeserializableEnumProperty, GeoMesaAvroGeomFormat, GeoMesaAvroVisibilityField}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.kafka.confluent.ConfluentFeatureSerializer.ConfluentFeatureMapper
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{InputStream, OutputStream}
import java.net.URL
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

class ConfluentFeatureSerializer(
    sft: SimpleFeatureType,
    schemaRegistryClient: SchemaRegistryClient,
    schemaOverride: Option[Schema] = None,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer with LazyLogging {

  private val schema = schemaOverride.getOrElse {
    val schemaId =
      Option(sft.getUserData.get(ConfluentMetadata.SchemaIdKey))
          .map(_.toString.toInt)
          .getOrElse {
            throw new IllegalStateException(s"Cannot create ConfluentFeatureSerializer because SimpleFeatureType " +
                s"'${sft.getTypeName}' does not have schema id at key '${ConfluentMetadata.SchemaIdKey}'")
          }
    schemaRegistryClient.getById(schemaId)
  }

  private val schemaValidationCheck = new AtomicBoolean(false)

  private val serializers = new ThreadLocal[ConfluentFeatureMapper]() {
    override def initialValue(): ConfluentFeatureMapper = {
      val mapper = new ConfluentFeatureMapper(sft, schema, schemaRegistryClient)
      if (schemaValidationCheck.compareAndSet(false, true)) {
        val violations = mapper.checkSchemaViolations()
        if (violations.nonEmpty) {
          logger.warn(
            "The following required schema fields are not mapped to any feature type attributes, " +
                s"and may cause errors during serialization: ${violations.mkString(", ")}")
        }
      }
      mapper
    }
  }

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature =
    serializers.get.read(id, bytes)

  override def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize("", bytes)

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    deserialize("", bytes, offset, length)

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = {
    val buf = if (offset == 0 && length == bytes.length) { bytes } else {
      val buf = Array.ofDim[Byte](length)
      System.arraycopy(bytes, offset, buf, 0, length)
      buf
    }
    deserialize(id, buf)
  }

  override def serialize(feature: SimpleFeature): Array[Byte] = serializers.get.write(feature)

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit = out.write(serialize(feature))

  // implement the following if we need them

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError()

  override def deserialize(id: String, in: InputStream): SimpleFeature =
    throw new NotImplementedError()
}

object ConfluentFeatureSerializer {

  def builder(sft: SimpleFeatureType, schemaRegistryUrl: URL, schemaOverride: Option[Schema] = None): Builder =
    new Builder(sft, schemaRegistryUrl, schemaOverride)

  class Builder private[ConfluentFeatureSerializer](
    sft: SimpleFeatureType,
    schemaRegistryUrl: URL,
    schemaOverride: Option[Schema] = None
  ) extends SimpleFeatureSerializer.Builder[Builder] {
    override def build(): ConfluentFeatureSerializer = {
      val client = new CachedSchemaRegistryClient(schemaRegistryUrl.toExternalForm, 100)
      new ConfluentFeatureSerializer(sft, client, schemaOverride, options.toSet)
    }
  }

  /**
   * Mapping between Avro schema and SimpleFeatureType
   *
   * @param sftIndex index of the field in the sft
   * @param schemaIndex index of the field in the avro schema
   * @param default default value defined in the avro schema
   * @param conversionToFeature convert from an avro value to a simple feature type attribute
   * @param conversionToAvro convert from a simple feature type attribute to an avro value
   */
  private case class FieldMapping(
      sftIndex: Int,
      schemaIndex: Int,
      default: Option[AnyRef],
      conversionToFeature: Option[AnyRef => AnyRef],
      conversionToAvro: Option[AnyRef => AnyRef]
    )

  /**
   * Converts between serialized Avro records and simple features
   *
   * @param sft simple feature type
   * @param schema avro schema
   * @param registry schema registry client
   */
  private class ConfluentFeatureMapper(sft: SimpleFeatureType, schema: Schema, registry: SchemaRegistryClient) {

    private val topic = KafkaDataStore.topic(sft)
    private val kafkaSerializer = new KafkaAvroSerializer(registry)
    private val kafkaDeserializer = new KafkaAvroDeserializer(registry)

    // feature type field index, schema field index and default value, any conversions necessary
    private val fieldMappings = sft.getAttributeDescriptors.asScala.map { d =>
      val conversion =
        if (classOf[Geometry].isAssignableFrom(d.getType.getBinding)) {
          Some(GeoMesaAvroGeomFormat.asInstanceOf[GeoMesaAvroDeserializableEnumProperty[AnyRef, AnyRef]])
        } else if (classOf[Date].isAssignableFrom(d.getType.getBinding)) {
          Some(GeoMesaAvroDateFormat.asInstanceOf[GeoMesaAvroDeserializableEnumProperty[AnyRef, AnyRef]])
        } else {
          None
        }

      val field = schema.getField(d.getLocalName)
      val conversionToFeature = conversion.map(_.getFieldReader(schema, field.name()))
      val conversionToAvro = conversion.map(_.getFieldWriter(schema, field.name()))

      FieldMapping(sft.indexOf(d.getLocalName), field.pos(), defaultValue(field), conversionToFeature, conversionToAvro)
    }

    // visibility field index in the avro schema
    private val visibilityField = schema.getFields.asScala.collectFirst {
      case f if Option(f.getProp(GeoMesaAvroVisibilityField.KEY)).exists(_.toBoolean) => f.pos()
    }

    // avro fields with default values that aren't part of the feature type
    private val defaultFields = schema.getFields.asScala.flatMap(f => defaultValue(f).map(v => f.pos() -> v)).filter {
      case (pos, _) => !fieldMappings.exists(_.schemaIndex == pos) && !visibilityField.contains(pos)
    }

    /**
     * Checks for required fields in the avro schema that are not part of the feature type
     * (i.e. will never be written)
     *
     * @return list of fields that will cause schema validation errors during serialization
     */
    def checkSchemaViolations(): Seq[String] = {
      val mappedPositions = fieldMappings.map(_.schemaIndex) ++ visibilityField.toSeq
      schema.getFields.asScala.collect {
        case f if requiredField(f) && !mappedPositions.contains(f.pos()) => f.name()
      }.toSeq
    }

    /**
     * Serialize a feature as Avro
     *
     * @param feature feature to serialize
     * @return
     */
    def write(feature: SimpleFeature): Array[Byte] = {
      val record = new GenericData.Record(schema)
      defaultFields.foreach { case (i, v) => record.put(i, v) }
      visibilityField.foreach { pos => record.put(pos, SecurityUtils.getVisibility(feature)) }
      fieldMappings.foreach { m =>
        try {
          feature.getAttribute(m.sftIndex) match {
            case null => m.default.foreach(d => record.put(m.schemaIndex, d))
            case v => record.put(m.schemaIndex, m.conversionToAvro.fold(v)(_.apply(v)))
          }
        } catch {
          case NonFatal(e) =>
            val d = sft.getDescriptor(m.sftIndex)
            val v = Try(feature.getAttribute(m.sftIndex))
            val s = schema.getField(d.getLocalName).schema()
            throw new RuntimeException(
              s"Cannot serialize field '${d.getLocalName}' with try-value '$v' into schema '$s':", e)
        }
      }

      kafkaSerializer.serialize(topic, record)
    }

    /**
     * Deserialize an Avro record into a feature
     *
     * @param id feature id
     * @param bytes serialized avro bytes
     * @return
     */
    def read(id: String, bytes: Array[Byte]): SimpleFeature = {
      val record = kafkaDeserializer.deserialize(topic, bytes).asInstanceOf[GenericRecord]
      val attributes = fieldMappings.map { m =>
        try {
          val value = record.get(m.schemaIndex)
          m.conversionToFeature match {
            case Some(c) if value != null => c.apply(value)
            case _ => value
          }
        } catch {
          case NonFatal(e) =>
            val d = sft.getDescriptor(m.sftIndex)
            throw new RuntimeException(
              s"Cannot deserialize field '${d.getLocalName}' into a '${d.getType.getBinding.getName}':", e)
        }
      }

      val feature = ScalaSimpleFeature.create(sft, id, attributes.toSeq: _*)

      // set the feature visibility if it exists
      visibilityField.foreach { field =>
        val vis = record.get(field)
        if (vis != null) {
          SecurityUtils.setFeatureVisibility(feature, vis.toString)
        }
      }

      feature
    }

    // filter out JNull - bug in kafka avro deserialization https://issues.apache.org/jira/browse/AVRO-1954
    private def defaultValue(f: Field): Option[AnyRef] =
      Option(f.defaultVal()).filterNot(_.isInstanceOf[JsonProperties.Null])

    private def requiredField(f: Field): Boolean = {
      defaultValue(f).isEmpty && {
        f.schema().getType match {
          case Type.NULL => false
          case Type.UNION => !f.schema().getTypes.contains(Type.NULL)
          case _ => true
        }
      }
    }
  }
}
