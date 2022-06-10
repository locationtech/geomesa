/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.kafka.confluent.ConfluentFeatureSerializer.ConfluentFeatureMapper
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.text.{DateParsing, WKBUtils, WKTUtils}
import org.locationtech.jts.geom.Geometry

import java.io.{InputStream, OutputStream}
import java.net.URL
import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
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

<<<<<<< HEAD
  import SchemaParser.{GeoMesaAvroDateFormat, GeoMesaAvroVisibilityField}
=======
  import SchemaParser.{GeoMesaAvroDateFormat, GeoMesaAvroDeserializableEnumProperty, GeoMesaAvroGeomFormat, GeoMesaAvroVisibilityField}
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

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

<<<<<<< HEAD
  /**
   * Mapping between Avro schema and SimpleFeatureType
   *
   * @param sftIndex index of the field in the sft
   * @param schemaIndex index of the field in the avro schema
   * @param default default value defined in the avro schema
   * @param conversion convert from an avro value to a simple feature type attribute, and vice-versa
   */
  private case class FieldMapping(
      sftIndex: Int,
      schemaIndex: Int,
      default: Option[AnyRef],
      conversion: Option[FieldConverter]
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
=======
class ConfluentFeatureSerializer(
    sft: SimpleFeatureType,
    schemaRegistryClient: SchemaRegistryClient,
    schemaOverride: Option[Schema] = None,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer with LazyLogging {

  private val kafkaAvroDeserializers = new ThreadLocal[KafkaAvroDeserializer]() {
    override def initialValue(): KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  }

  private val featureReaders = new ThreadLocal[ConfluentFeatureReader]() {
    override def initialValue(): ConfluentFeatureReader = {
      val schema = schemaOverride.getOrElse {
        val schemaId = Option(sft.getUserData.get(ConfluentMetadata.SchemaIdKey))
          .map(_.asInstanceOf[String].toInt).getOrElse {
          throw new IllegalStateException(s"Cannot create ConfluentFeatureSerializer because SimpleFeatureType " +
            s"'${sft.getTypeName}' does not have schema id at key '${ConfluentMetadata.SchemaIdKey}'")
        }
        schemaRegistryClient.getById(schemaId)
      }
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))

    // feature type field index, schema field index and default value, any conversions necessary
    private val fieldMappings = sft.getAttributeDescriptors.asScala.map { d =>
      val field = schema.getField(d.getLocalName)

<<<<<<< HEAD
      val conversion =
        if (classOf[Geometry].isAssignableFrom(d.getType.getBinding)) {
          lazy val union = field.schema.getTypes.asScala.map(_.getType).filter(_ != Schema.Type.NULL).toSet
          field.schema.getType match {
            case Schema.Type.STRING => Some(WktConverter)
            case Schema.Type.BYTES  => Some(WkbConverter)
            case Schema.Type.UNION if union == Set(Schema.Type.STRING) => Some(WktConverter)
            case Schema.Type.UNION if union == Set(Schema.Type.BYTES)  => Some(WkbConverter)
            case _ => throw new IllegalStateException(s"Found a geometry field with an invalid schema: $field")
=======
  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = {
    val record = kafkaAvroDeserializers.get.deserialize("", bytes).asInstanceOf[GenericRecord]

    val feature = featureReaders.get.read(id, record)

    // set the feature visibility if it exists
    sft.getUserData.get(GeoMesaAvroVisibilityField.KEY) match {
      case null => // no-op
      case fieldName =>
        record.get(fieldName.toString) match {
          case null => // no-op
          case vis => SecurityUtils.setFeatureVisibility(feature, vis.toString)
        }

    }

    feature
  }

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

  // implement the following if we need them

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError()

  override def deserialize(id: String, in: InputStream): SimpleFeature =
    throw new NotImplementedError()

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  // precompute the deserializer for each field in the SFT to simplify the actual deserialization
  private class ConfluentFeatureReader(sft: SimpleFeatureType, schema: Schema) {

    def read(id: String, record: GenericRecord): SimpleFeature = {
      val attributes = fieldReaders.map { fieldReader =>
        try {
          Option(record.get(fieldReader.fieldName)).map(fieldReader.reader.apply).orNull
        } catch {
          case NonFatal(ex) =>
            throw ConfluentFeatureReader.DeserializationException(fieldReader.fieldName, fieldReader.clazz, ex)
        }
      }

      ScalaSimpleFeature.create(sft, id, attributes: _*)
    }

    private val fieldReaders: Seq[ConfluentFeatureReader.FieldReader[_]] = {
      sft.getAttributeDescriptors.asScala.map { descriptor =>
        val fieldName = descriptor.getLocalName

        val reader =
          if (classOf[Geometry].isAssignableFrom(descriptor.getType.getBinding)) {
            GeoMesaAvroGeomFormat.getFieldReader(schema, fieldName)
          } else if (classOf[Date].isAssignableFrom(descriptor.getType.getBinding)) {
            GeoMesaAvroDateFormat.getFieldReader(schema, fieldName)
          } else {
            value: AnyRef => value
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
          }
        } else if (classOf[Date].isAssignableFrom(d.getType.getBinding)) {
          d.getUserData.get(GeoMesaAvroDateFormat.KEY) match {
            case GeoMesaAvroDateFormat.ISO_DATE     => Some(IsoDateConverter)
            case GeoMesaAvroDateFormat.ISO_DATETIME => Some(IsoDateTimeConverter)
            case GeoMesaAvroDateFormat.EPOCH_MILLIS => Some(EpochMillisConverter)
            case null /* avro logical date type */  => Some(EpochMillisConverter)
            case _ =>
              throw new IllegalStateException(s"Found a date field with no format defined:" +
                s" ${d.getLocalName} ${d.getUserData.asScala.mkString(", ")}")
          }
        } else {
          None
=======
class ConfluentFeatureSerializer(
    sft: SimpleFeatureType,
    schemaRegistryClient: SchemaRegistryClient,
    schemaOverride: Option[Schema] = None,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer with LazyLogging {

  private val kafkaAvroDeserializers = new ThreadLocal[KafkaAvroDeserializer]() {
    override def initialValue(): KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  }

  private val featureReaders = new ThreadLocal[ConfluentFeatureReader]() {
    override def initialValue(): ConfluentFeatureReader = {
      val schema = schemaOverride.getOrElse {
        val schemaId = Option(sft.getUserData.get(ConfluentMetadata.SchemaIdKey))
          .map(_.asInstanceOf[String].toInt).getOrElse {
          throw new IllegalStateException(s"Cannot create ConfluentFeatureSerializer because SimpleFeatureType " +
            s"'${sft.getTypeName}' does not have schema id at key '${ConfluentMetadata.SchemaIdKey}'")
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
        }

<<<<<<< HEAD
      FieldMapping(sft.indexOf(d.getLocalName), field.pos(), defaultValue(field), conversion)
=======
      val field = schema.getField(d.getLocalName)
      val conversionToFeature = conversion.map(_.getFieldReader(schema, field.name()))
      val conversionToAvro = conversion.map(_.getFieldWriter(schema, field.name()))

<<<<<<< HEAD
      FieldMapping(sft.indexOf(d.getLocalName), field.pos(), defaultValue(field), conversionToFeature, conversionToAvro)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = {
    val record = kafkaAvroDeserializers.get.deserialize("", bytes).asInstanceOf[GenericRecord]

    val feature = featureReaders.get.read(id, record)

    // set the feature visibility if it exists
    sft.getUserData.get(GeoMesaAvroVisibilityField.KEY) match {
      case null => // no-op
      case fieldName =>
        record.get(fieldName.toString) match {
          case null => // no-op
          case vis => SecurityUtils.setFeatureVisibility(feature, vis.toString)
        }

>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
    }

<<<<<<< HEAD
    // visibility field index in the avro schema
    private val visibilityField = schema.getFields.asScala.collectFirst {
      case f if Option(f.getProp(GeoMesaAvroVisibilityField.KEY)).exists(_.toBoolean) => f.pos()
    }

<<<<<<< HEAD
    // avro fields with default values that aren't part of the feature type
    private val defaultFields = schema.getFields.asScala.flatMap(f => defaultValue(f).map(v => f.pos() -> v)).filter {
      case (pos, _) => !fieldMappings.exists(_.schemaIndex == pos) && !visibilityField.contains(pos)
    }
=======
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

  // implement the following if we need them
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))

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

<<<<<<< HEAD
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
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30fda14bf2 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c6591bc391 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> eef10da74 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 60bcd014c8 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
=======
>>>>>>> 1c64e1adc3 (GEOMESA-3198 Kafka streams integration (#2854))
=======
  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = {
    val record = kafkaAvroDeserializers.get.deserialize("", bytes).asInstanceOf[GenericRecord]

    val feature = featureReaders.get.read(id, record)

    // set the feature visibility if it exists
    sft.getUserData.get(GeoMesaAvroVisibilityField.KEY) match {
      case null => // no-op
      case fieldName =>
        record.get(fieldName.toString) match {
          case null => // no-op
          case vis => SecurityUtils.setFeatureVisibility(feature, vis.toString)
        }

    }

    feature
  }

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

  // implement the following if we need them

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError()

<<<<<<< HEAD
>>>>>>> d657014c8 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> f01ac238e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 1b8cbf843 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 30fda14bf2 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 3be3e5d3e (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> d98371fa50 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> 73f3a8cb6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> c6591bc391 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> eef10da74 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 769842a4bf (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 60bcd014c8 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 1c64e1adc3 (GEOMESA-3198 Kafka streams integration (#2854))
  override def deserialize(id: String, in: InputStream): SimpleFeature =
    throw new NotImplementedError()

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  // precompute the deserializer for each field in the SFT to simplify the actual deserialization
  private class ConfluentFeatureReader(sft: SimpleFeatureType, schema: Schema) {

    def read(id: String, record: GenericRecord): SimpleFeature = {
      val attributes = fieldReaders.map { fieldReader =>
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
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
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f01ac238e (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c6591bc391 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b8cbf843 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 30fda14bf2 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 73f3a8cb6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> c6591bc391 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c64e1adc3 (GEOMESA-3198 Kafka streams integration (#2854))
        try {
          feature.getAttribute(m.sftIndex) match {
            case null => m.default.foreach(d => record.put(m.schemaIndex, d))
            case v => record.put(m.schemaIndex, m.conversion.fold(v)(_.featureToRecord(v)))
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
          val v = record.get(m.schemaIndex)
          m.conversion match {
            case None => v
            case Some(c) => c.recordToFeature(v)
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

  /**
   * Converts between avro and feature attribute values
   */
  private sealed trait FieldConverter {
    def recordToFeature(value: AnyRef): AnyRef
    def featureToRecord(value: AnyRef): AnyRef
  }

  /**
   * Converts WKT text fields
   */
  private case object WktConverter extends FieldConverter {
    override def recordToFeature(value: AnyRef): AnyRef = {
      // note: value is an org.apache.avro.util.Utf8
      if (value == null) { null } else { WKTUtils.read(value.toString) }
    }

    override def featureToRecord(value: AnyRef): AnyRef =
      if (value == null) { null } else { WKTUtils.write(value.asInstanceOf[Geometry]) }
  }

  /**
   * Converts WKB bytes fields
   */
  private case object WkbConverter extends FieldConverter {
    override def recordToFeature(value: AnyRef): AnyRef =
      if (value == null) { null } else { WKBUtils.read(unwrap(value.asInstanceOf[ByteBuffer])) }

    override def featureToRecord(value: AnyRef): AnyRef =
      if (value == null) { null } else { ByteBuffer.wrap(WKBUtils.write(value.asInstanceOf[Geometry])) }

    private def unwrap(buf: ByteBuffer): Array[Byte] = {
      if (buf.hasArray && buf.arrayOffset() == 0 && buf.limit() == buf.array().length) {
        buf.array()
      } else {
        val array = Array.ofDim[Byte](buf.limit())
        buf.get(array)
        array
      }
    }
  }

  /**
   * Converts ISO_DATE formatted string fields
   */
  private case object IsoDateConverter extends FieldConverter {
    override def recordToFeature(value: AnyRef): AnyRef = {
      if (value == null) { null } else {
        // note: value is an org.apache.avro.util.Utf8
        DateParsing.parseDate(value.toString, DateTimeFormatter.ISO_DATE)
      }
    }

    override def featureToRecord(value: AnyRef): AnyRef = {
      if (value == null) { null } else {
        DateParsing.formatDate(value.asInstanceOf[Date], DateTimeFormatter.ISO_DATE)
      }
    }
  }

  /**
   * Converts ISO_DATE_TIME formatted string fields
   */
  private case object IsoDateTimeConverter extends FieldConverter {
    override def recordToFeature(value: AnyRef): AnyRef = {
      if (value == null) { null } else {
        // note: value is an org.apache.avro.util.Utf8
        DateParsing.parseDate(value.toString, DateTimeFormatter.ISO_DATE_TIME)
      }
    }

    override def featureToRecord(value: AnyRef): AnyRef = {
      if (value == null) { null } else {
        DateParsing.formatDate(value.asInstanceOf[Date], DateTimeFormatter.ISO_DATE_TIME)
      }
    }
  }

  /**
   * Converts milliseconds since epoch long fields
   */
  private case object EpochMillisConverter extends FieldConverter {
    override def recordToFeature(value: AnyRef): AnyRef =
      if (value == null) { null } else { new Date(value.asInstanceOf[java.lang.Long]) }

    override def featureToRecord(value: AnyRef): AnyRef =
      if (value == null) { null } else { Long.box(value.asInstanceOf[Date].getTime) }
  }

}
