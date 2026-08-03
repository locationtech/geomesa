/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage

import com.google.gson._
import com.typesafe.config.ConfigFactory
import org.apache.iceberg.Table
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.Suffixes.Memory
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigSource}

import java.lang.reflect.Type
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

package object core {

  val CacheDurationProperty: SystemProperty = SystemProperty("geomesa.fs.file.cache.duration", "15 minutes")
  val FileValidationEnabled: SystemProperty = SystemProperty("geomesa.fs.validate.file", "false")

  private val gson =
    new GsonBuilder()
      .registerTypeAdapter(classOf[PartitionKey], PartitionKey.PartitionKeySerializer)
      .registerTypeAdapter(classOf[Partition], Partition.PartitionSerializer)
      .disableHtmlEscaping()
      .create()

  /**
   * Creates a new simple feature type with the namespace in the simple feature type name
   *
   * @param sft simple feature type
   * @param namespace optional namespace
   * @return
   */
  def namespaced(sft: SimpleFeatureType, namespace: Option[String]): SimpleFeatureType =
    namespace.map(ns => SimpleFeatureTypes.renameSft(sft, s"$ns:${sft.getTypeName}")).getOrElse(sft)


  implicit class RichConf(val conf: Map[String, String]) extends AnyRef {
    def getWriterMaxOpenPartitions: Int =
      conf.get(FileSystemStorage.WriterMaxOpenPartitions).fold(FileSystemStorage.WriterMaxOpenPartitionsDefault)(_.toInt)
  }

  object Metadata {

    val PropertyPrefix = "geomesa.props."

    val TargetFileSize = "target-file-size"

    def get(table: Table, key: String): Option[String] = Option(table.properties().get(s"$PropertyPrefix$key"))
    def set(table: Table, key: String, value: String): Unit = {
      val update = table.updateProperties()
      if (value == null) {
        update.remove(s"$PropertyPrefix$key")
      } else {
        update.set(s"$PropertyPrefix$key", value)
      }
      update.commit()
    }
  }

  /**
   * A partition
   *
   * @param values set of dimensions that make up the partition
   */
  case class Partition(values: Seq[PartitionKey]) {
    override lazy val toString: String = gson.toJson(this)
  }

  object Partition {

    val None: Partition = Partition(Seq.empty[PartitionKey])

    /**
     * Create a partition from a json-encoded string
     *
     * @param encoded json representation of the partition
     * @return
     */
    def apply(encoded: String): Partition = {
      try { gson.fromJson(encoded, classOf[Partition]) } catch {
        case NonFatal(e) => throw new RuntimeException(s"Invalid partition json: $encoded", e)
      }
    }

    /**
     * Json serializer for partitions
     */
    object PartitionSerializer extends JsonSerializer[Partition] with JsonDeserializer[Partition] {

      override def serialize(src: Partition, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
        val array = new JsonArray(src.values.size)
        src.values.foreach { value =>
          array.add(context.serialize(value))
        }
        array
      }

      override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Partition = {
        val array = json.getAsJsonArray
        val values = Seq.newBuilder[PartitionKey]
        var i = 0
        while (i < array.size()) {
          values += context.deserialize(array.get(i), classOf[PartitionKey])
          i += 1
        }
        Partition(values.result())
      }
    }
  }

  /**
   * A partition tag. A set of tags makes up a partition
   *
   * @param name partition scheme
   * @param value partition value
   */
  case class PartitionKey(name: String, value: String) {
    override lazy val toString: String = gson.toJson(this)
  }

  object PartitionKey {

    /**
     * Create a partition key from a json-encoded string
     *
     * @param encoded json representation of the partition key
     * @return
     */
    def apply(encoded: String): PartitionKey = {
      try { gson.fromJson(encoded, classOf[PartitionKey]) } catch {
        case NonFatal(e) => throw new RuntimeException(s"Invalid partition key json: $encoded", e)
      }
    }

    /**
     * Json serializer for partition keys
     */
    object PartitionKeySerializer extends JsonSerializer[PartitionKey] with JsonDeserializer[PartitionKey] {

      override def serialize(src: PartitionKey, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty("name", src.name)
        obj.addProperty("value", src.value)
        obj
      }

      override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): PartitionKey = {
        val obj = json.getAsJsonObject
        val name = obj.getAsJsonPrimitive("name").getAsString
        val value = obj.getAsJsonPrimitive("value").getAsString
        PartitionKey(name, value)
      }
    }
  }

  private lazy implicit val SchemeOptionsConvert: ConfigConvert[SchemeOptions] = deriveConvert[SchemeOptions]
  private lazy implicit val NamedOptionsConvert: ConfigConvert[NamedOptions] = deriveConvert[NamedOptions]

  object StorageKeys {
    val SchemeKey    = "geomesa.fs.scheme"
    val FileSizeKey  = "geomesa.fs.file-size"
    val ObserversKey = "geomesa.fs.observers"
  }

  /**
   * Implicit methods to set/retrieve storage configuration options in SimpleFeatureType user data
   *
   * @param sft simple feature type
   */
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {
    import StorageKeys._

    def setScheme(names: String): Unit = sft.getUserData.put(SchemeKey, names)
    def removeScheme(): Option[Seq[String]] = {
      remove(SchemeKey).map { scheme =>
        // back compatible check for old json-serialized schemes
        if (scheme.trim.startsWith("{")) {
          try {
            def result(name: String, options: Map[String, String]): Seq[String] = {
              val opts = options.map { case (k, v) => s"$k=$v" }.mkString(":")
              name.split(",").toSeq.map(n => s"$n:$opts")
            }
            val source = ConfigSource.fromConfig(ConfigFactory.parseString(scheme))
            source.load[SchemeOptions] match {
              case Right(o) => result(o.scheme, o.options)
              case Left(_) =>
                val n = source.loadOrThrow[NamedOptions]
                result(n.name, n.options)
            }
          } catch {
            case NonFatal(e) => throw new RuntimeException(s"Could not parse legacy scheme options: $scheme", e)
          }
        } else {
          scheme.split(",").toSeq
        }
      }
    }

    def setTargetFileSize(size: String): Unit = {
      // validate input
      Memory.bytes(size).failed.foreach(e => throw new IllegalArgumentException("Invalid file size", e))
      sft.getUserData.put(FileSizeKey, size)
    }
    def removeTargetFileSize(): Option[Long] = {
      remove(FileSizeKey).map { s =>
        Memory.bytes(s) match {
          case Success(b) => b
          case Failure(e) => throw new IllegalArgumentException("Invalid file size", e)
        }
      }
    }

    def setObservers(names: Seq[String]): Unit = sft.getUserData.put(ObserversKey, names.mkString(","))
    def getObservers: Seq[String] = {
      val obs = sft.getUserData.get(ObserversKey).asInstanceOf[String]
      if (obs == null || obs.isEmpty) { Seq.empty } else { obs.split(",") }
    }

    private def remove(key: String): Option[String] = Option(sft.getUserData.remove(key).asInstanceOf[String])
  }

  // kept around for back compatibility with encoded partition schemes
  private case class SchemeOptions(scheme: String, options: Map[String, String] = Map.empty)
  private case class NamedOptions(name: String, options: Map[String, String] = Map.empty)
}
