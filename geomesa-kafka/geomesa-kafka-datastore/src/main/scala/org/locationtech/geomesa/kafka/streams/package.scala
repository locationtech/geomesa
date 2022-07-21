/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.apache.kafka.streams.processor.StreamPartitioner
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.streams.MessageAction.MessageAction
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer

package object streams {

  /**
   * Data model for a GeoMesa data store message, used as the value in a Kafka record
   *
   * @param action message action
   * @param attributes attributes of the simple feature represented by this message
   * @param userData user data of the simple feature represented by this message
   */
  case class GeoMesaMessage(action: MessageAction, attributes: Seq[AnyRef], userData: Map[String, String] = Map.empty)

  object GeoMesaMessage {

    /**
     * Create an upsert message
     *
     * @param attributes feature attribute values
     * @return
     */
    def upsert(attributes: Seq[AnyRef]): GeoMesaMessage = GeoMesaMessage(MessageAction.Upsert, attributes)

    /**
     * Create an upsert message
     *
     * @param attributes feature attribute values
     * @param userData feature user data
     * @return
     */
    def upsert(attributes: Seq[AnyRef], userData: Map[String, String]): GeoMesaMessage =
      GeoMesaMessage(MessageAction.Upsert, attributes, userData)

    /**
     * Create a delete message
     *
     * @return
     */
    def delete(): GeoMesaMessage = GeoMesaMessage(MessageAction.Delete, Seq.empty)
  }

  /**
   * Types of messages
   */
  object MessageAction extends Enumeration {
    type MessageAction = Value
    val Upsert, Delete = Value
  }

  /**
   * Trait for provided metadata about a feature type topic
   */
  trait HasTopicMetadata {

    /**
     * Gets the topic associated with a feature type
     *
     * @param typeName feature type name
     * @return
     */
    def topic(typeName: String): String

    /**
     * Gets the partitioning associated with a feature type
     *
     * @param typeName feature type name
     * @return true if Kafka default partitioning is used, false if custom partitioning is used
     */
    def usesDefaultPartitioning(typeName: String): Boolean
  }

  /**
   * Kafka partitioner for GeoMesa messages, to make sure all updates for a given
   * feature go to the same partition
   */
  class GeoMessageStreamPartitioner extends StreamPartitioner[String, GeoMesaMessage] {
    override def partition(
        topic: String,
        key: String,
        value: GeoMesaMessage,
        numPartitions: Int): Integer = {
      GeoMessageSerializer.partition(numPartitions,
        if (key == null) { null } else { key.getBytes(StandardCharsets.UTF_8) })
    }
  }

  /**
   * Cache for serializers and topic names
   *
   * @param params data store params
   */
  class SerializerCache(params: java.util.Map[String, _]) extends HasTopicMetadata {

    private val metadataByTypeName = new ConcurrentHashMap[String, SchemaMetadata]()
    private val serializersByTopic = new ConcurrentHashMap[String, GeoMesaMessageSerializer]()

    private val metadataLoader = new java.util.function.Function[String, SchemaMetadata]() {
      override def apply(typeName: String): SchemaMetadata = loadMetadata(typeName)
    }

    private val serializerLoader = new java.util.function.Function[String, GeoMesaMessageSerializer]() {
      override def apply(topic: String): GeoMesaMessageSerializer = loadSerializer(topic)
    }

    // track last-used serializer so we don't have to look them up by hash each
    // time if we're just reading/writing to one topic (which is the standard use-case)
    @volatile
    private var last: (String, GeoMesaMessageSerializer) = ("", null)

    override def topic(typeName: String): String = metadataByTypeName.computeIfAbsent(typeName, metadataLoader).topic

    override def usesDefaultPartitioning(typeName: String): Boolean =
      metadataByTypeName.computeIfAbsent(typeName, metadataLoader).usesDefaultPartitioning

    /**
     * Gets the serializer associated with a topic
     *
     * @param topic kafka topic name
     * @return
     */
    def serializer(topic: String): GeoMesaMessageSerializer = {
      val (lastTopic, lastSerializer) = last
      if (lastTopic == topic) { lastSerializer } else {
        val serializer = serializersByTopic.computeIfAbsent(topic, serializerLoader)
        // should be thread-safe due to volatile
        last = (topic, serializer)
        serializer
      }
    }

    private def loadMetadata(typeName: String): SchemaMetadata = {
      withDataStore { ds =>
        ds.getSchema(typeName) match {
          case sft => SchemaMetadata(KafkaDataStore.topic(sft), KafkaDataStore.usesDefaultPartitioning(sft))
          case null =>
            throw new IllegalArgumentException(
              s"Schema '$typeName' does not exist in the configured store. " +
                  s"Available schemas: ${ds.getTypeNames.mkString(", ")}")
        }
      }
    }

    private def loadSerializer(topic: String): GeoMesaMessageSerializer = {
      withDataStore { ds =>
        val topics = ArrayBuffer.empty[String]
        // order so that we check the most likely ones first
        val typeNames = ds.getTypeNames.partition(_.contains(topic)) match {
          case (left, right) => left ++ right
        }
        var i = 0
        while (i < typeNames.length) {
          val sft = ds.getSchema(typeNames(i))
          KafkaDataStore.topic(sft) match {
            case t if t == topic =>
              val internal = ds.serialization(sft, ds.config.serialization, `lazy` = false).serializer
              return new GeoMesaMessageSerializer(sft, internal)

            case t => topics += t
          }
          i += 1
        }
        throw new IllegalArgumentException(
          s"Topic '$topic' does not exist in the configured store. Available topics: ${topics.mkString(", ")}")
      }
    }

    private def withDataStore[T](fn: KafkaDataStore => T): T = {
      WithClose(DataStoreFinder.getDataStore(params)) {
        case ds: KafkaDataStore => fn(ds)
        case null => throw new IllegalArgumentException("Could not load data store with provided params")
        case ds => throw new IllegalArgumentException(s"Expected a KafkaDataStore but got ${ds.getClass.getName}")
      }
    }

    private case class SchemaMetadata(topic: String, usesDefaultPartitioning: Boolean)
  }

  /**
   * Serializer for GeoMesaMessages
   *
   * @param sft feature type
   * @param internal nested serializer
   */
  class GeoMesaMessageSerializer(val sft: SimpleFeatureType, val internal: SimpleFeatureSerializer) {

    import scala.collection.JavaConverters._

    private val converters: Array[AnyRef => AnyRef] =
      sft.getAttributeDescriptors.toArray(Array.empty[AttributeDescriptor]).map { d =>
        val binding = d.getType.getBinding.asInstanceOf[Class[_ <: AnyRef]]
        (in: AnyRef) => FastConverter.convert(in, binding)
      }

    def serialize(data: GeoMesaMessage): Array[Byte] = {
      data.action match {
        case MessageAction.Upsert => internal.serialize(wrap(data))
        case MessageAction.Delete => null
        case null => throw new NullPointerException("action is null")
        case _ => throw new NotImplementedError(s"No serialization implemented for action '${data.action}'")
      }
    }

    def deserialize(data: Array[Byte]): GeoMesaMessage = {
      if (data == null || data.isEmpty) { GeoMesaMessage.delete() } else {
        val feature = internal.deserialize(data)
        val userData = if (feature.getUserData.isEmpty) { Map.empty[String, String] } else {
          val builder = Map.newBuilder[String, String]
          feature.getUserData.asScala.foreach {
            case (k: String, v: String) => builder += k -> v
            case (k, v) => builder += k.toString -> v.toString
          }
          builder.result
        }
        GeoMesaMessage.upsert(feature.getAttributes.asScala.toSeq, userData)
      }
    }

    /**
     * Wrap a message as a simple feature
     *
     * @param message message
     * @return
     */
    def wrap(message: GeoMesaMessage): SimpleFeature =
      new SerializableFeature(converters, message.attributes.toIndexedSeq, message.userData)
  }

  /**
   * SimpleFeature skeleton that only provides the methods required for GeoMesa serialization, which are:
   *   * `def getAttribute(i: Int): AnyRef`
   *   * `def getUserData: java.util.Map[AnyRef, AnyRef]`
   *
   * See
   *   * @see [[org.locationtech.geomesa.features.kryo.impl.KryoFeatureSerialization#writeFeature]]
   *   * @see [[org.locationtech.geomesa.features.avro.AvroSimpleFeatureWriter#write]]
   *
   * @param converters attribute converters to enforce feature type schema
   * @param attributes message attributes
   */
  // noinspection NotImplementedCode
  private[streams] class SerializableFeature(
      converters: Array[AnyRef => AnyRef],
      attributes: IndexedSeq[AnyRef],
      userData: Map[String, String]
    ) extends SimpleFeature {

    import scala.collection.JavaConverters._

    override def getAttribute(i: Int): AnyRef = converters(i).apply(attributes(i))
    override def getUserData: java.util.Map[AnyRef, AnyRef] =
      userData.asJava.asInstanceOf[java.util.Map[AnyRef, AnyRef]]

    override def getID: String = ???
    override def getType: SimpleFeatureType = ???
    override def getFeatureType: SimpleFeatureType = ???
    override def getAttributes: java.util.List[AnyRef] = ???
    override def setAttributes(list:java.util.List[AnyRef]): Unit = ???
    override def setAttributes(objects: Array[AnyRef]): Unit = ???
    override def getAttribute(s: String): AnyRef = ???
    override def setAttribute(s: String, o: Any): Unit = ???
    override def getAttribute(name: Name): AnyRef = ???
    override def setAttribute(name: Name, o: Any): Unit = ???
    override def setAttribute(i: Int, o: Any): Unit = ???
    override def getAttributeCount: Int = ???
    override def getDefaultGeometry: AnyRef = ???
    override def setDefaultGeometry(o: Any): Unit = ???
    override def getIdentifier: FeatureId = ???
    override def getBounds: BoundingBox = ???
    override def getDefaultGeometryProperty: GeometryAttribute = ???
    override def setDefaultGeometryProperty(geometryAttribute: GeometryAttribute): Unit = ???
    override def setValue(collection:java.util.Collection[Property]): Unit = ???
    override def getValue:java.util.Collection[_ <: Property] = ???
    override def getProperties(name: Name):java.util.Collection[Property] = ???
    override def getProperty(name: Name): Property = ???
    override def getProperties(s: String):java.util.Collection[Property] = ???
    override def getProperties:java.util.Collection[Property] = ???
    override def getProperty(s: String): Property = ???
    override def validate(): Unit = ???
    override def getDescriptor: AttributeDescriptor = ???
    override def setValue(o: Any): Unit = ???
    override def getName: Name = ???
    override def isNillable: Boolean = ???
  }
}
