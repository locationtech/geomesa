/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import java.io.Writer
import java.util.{Base64, Date, UUID}

import com.google.gson.stream.JsonWriter
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Serializer to write out geojson from simple features. There are two valid usage patterns.
 *
 * To encode a feature collection with an array of features:
 *
 * &lt;ol&gt;
 *   &lt;li&gt;`startFeatureCollection`&lt;/li&gt;
 *   &lt;li&gt;`write` (0-n)&lt;/li&gt;
 *   &lt;li&gt;`endFeatureCollection`&lt;/li&gt;
 * &lt;/ol&gt;
 *
 * To encode individual features:
 *
 * &lt;ol&gt;
 *   &lt;li&gt;`write` (0-n)&lt;/li&gt;
 * &lt;/ol&gt;
 *
 * Note that encoding individual features is not valid json, so generally you would
 * want to split up the output after each feature.
 *
 * @param sft simple feature type
 */
class GeoJsonSerializer(sft: SimpleFeatureType) extends LazyLogging {

  import GeoJsonSerializer._

  private val defaultGeomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)

  private val geometryWriter = new GeometryWriter("geometry", defaultGeomIndex)

  private val attributeWriters = Seq.tabulate(sft.getAttributeCount - 1) { i =>
    GeoJsonSerializer.createAttributeWriter(sft, if (i < defaultGeomIndex) { i } else { i + 1 })
  }

  /**
   * Start a feature collection type
   *
   * @param writer json writer
   */
  def startFeatureCollection(writer: JsonWriter): Unit =
    writer.beginObject().name("type").value("FeatureCollection").name("features").beginArray()

  /**
   * Write a single feature
   *
   * @param writer json writer
   * @param feature feature
   */
  def write(writer: JsonWriter, feature: SimpleFeature): Unit = {
    writer.beginObject()
    writer.name("type").value("Feature")
    writer.name("id").value(feature.getID)
    geometryWriter.apply(feature, writer)
    writer.name("properties").beginObject()
    attributeWriters.foreach(_.apply(feature, writer))
    writer.endObject()
    writer.endObject()
  }

  /**
   * End a previously started feature collection
   *
   * @param writer json writer
   */
  def endFeatureCollection(writer: JsonWriter): Unit = writer.endArray().endObject()
}

object GeoJsonSerializer extends LazyLogging {

  private val geometryWriter = {
    val writer = new org.locationtech.jts.io.geojson.GeoJsonWriter()
    writer.setEncodeCRS(false)
    writer
  }

  /**
   * Create a JsonWriter from a standard writer.
   *
   * Note that closing the JsonWriter will close the wrapped writer. Technically the JsonWriter does not need
   * to be closed if you close the wrapped writer directly, although this is mainly a gson implementation
   * detail and may change in the future.
   *
   * @param wrapped writer
   * @return
   */
  def writer(wrapped: Writer): JsonWriter = {
    val writer = new JsonWriter(wrapped)
    writer.setSerializeNulls(true)
    writer.setLenient(true) // allow multiple top-level objects
    writer
  }

  private def createAttributeWriter(sft: SimpleFeatureType, i: Int): JsonAttributeWriter = {
    val descriptor = sft.getDescriptor(i)
    val name = descriptor.getLocalName
    val bindings = ObjectType.selectType(descriptor)

    bindings.head match {
      case ObjectType.STRING if bindings.last == ObjectType.JSON => new JsonStringWriter(name, i)
      case ObjectType.STRING   => new StringWriter(name, i)
      case ObjectType.INT      => new NumberWriter(name, i)
      case ObjectType.LONG     => new NumberWriter(name, i)
      case ObjectType.FLOAT    => new NumberWriter(name, i)
      case ObjectType.DOUBLE   => new NumberWriter(name, i)
      case ObjectType.DATE     => new DateWriter(name, i)
      case ObjectType.GEOMETRY => new GeometryWriter(name, i)
      case ObjectType.UUID     => new UuidWriter(name, i)
      case ObjectType.BYTES    => new BytesWriter(name, i)
      case ObjectType.BOOLEAN  => new BooleanWriter(name, i)
      case ObjectType.LIST     => new ListWriter(name, i, bindings(1))
      case ObjectType.MAP      => new MapWriter(name, i, bindings(1), bindings(2))

      case _ =>
        logger.warn(s"Dropping unsupported attribute '${SimpleFeatureTypes.encodeDescriptor(sft, descriptor)}'")
        NoopWriter
    }
  }

  sealed private trait JsonAttributeWriter {
    def apply(feature: SimpleFeature, writer: JsonWriter): Unit
  }

  private object NoopWriter extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit = {}
  }

  private class StringWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit =
      writer.name(name).value(feature.getAttribute(i).asInstanceOf[String])
  }

  private class JsonStringWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit =
      writer.name(name).beginObject().jsonValue(feature.getAttribute(i).asInstanceOf[String]).endObject()
  }

  private class NumberWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit =
      writer.name(name).value(feature.getAttribute(i).asInstanceOf[Number])
  }

  private class BooleanWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit =
      writer.name(name).value(feature.getAttribute(i).asInstanceOf[java.lang.Boolean])
  }

  private class DateWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit = {
      writer.name(name)
      val date = feature.getAttribute(i).asInstanceOf[Date]
      if (date == null) { writer.nullValue() } else {
        writer.value(DateParsing.formatDate(date))
      }
    }
  }

  private class GeometryWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit = {
      writer.name(name)
      val geom = feature.getAttribute(i).asInstanceOf[Geometry]
      if (geom == null) { writer.nullValue() } else {
        writer.jsonValue(geometryWriter.write(geom))
      }
    }
  }

  private class UuidWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit = {
      writer.name(name)
      val uuid = feature.getAttribute(i).asInstanceOf[UUID]
      if (uuid == null) { writer.nullValue() } else {
        writer.value(uuid.toString)
      }
    }
  }

  private class BytesWriter(name: String, i: Int) extends JsonAttributeWriter {
    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit = {
      writer.name(name)
      val bytes = feature.getAttribute(i).asInstanceOf[Array[Byte]]
      if (bytes == null) { writer.nullValue() } else {
        writer.value(Base64.getEncoder.encodeToString(bytes))
      }
    }
  }

  private class ListWriter(name: String, i: Int, binding: ObjectType) extends JsonAttributeWriter {

    import scala.collection.JavaConverters._

    private val elementWriter: (JsonWriter, Any) => Unit = subWriter(binding)

    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit = {
      writer.name(name)
      val list = feature.getAttribute(i).asInstanceOf[java.util.List[_]]
      if (list == null) {
        writer.nullValue()
      } else {
        writer.beginArray()
        list.asScala.foreach(elementWriter.apply(writer, _))
        writer.endArray()
      }
    }
  }

  private class MapWriter(name: String, i: Int, keyBinding: ObjectType, valueBinding: ObjectType)
      extends JsonAttributeWriter {

    import scala.collection.JavaConverters._

    private val valueWriter: (JsonWriter, Any) => Unit = subWriter(valueBinding)

    override def apply(feature: SimpleFeature, writer: JsonWriter): Unit = {
      writer.name(name)
      val map = feature.getAttribute(i).asInstanceOf[java.util.Map[_, _]]
      if (map == null) {
        writer.nullValue()
      } else {
        writer.beginObject()
        map.asScala.foreach { case (k, v) =>
          writer.name(k.toString)
          valueWriter(writer, v)
        }
        writer.endObject()
      }
    }
  }

  private def subWriter(binding: ObjectType): (JsonWriter, Any) => Unit = binding match {
    case ObjectType.INT      => (writer, elem) => writer.value(elem.asInstanceOf[Number])
    case ObjectType.LONG     => (writer, elem) => writer.value(elem.asInstanceOf[Number])
    case ObjectType.FLOAT    => (writer, elem) => writer.value(elem.asInstanceOf[Number])
    case ObjectType.DOUBLE   => (writer, elem) => writer.value(elem.asInstanceOf[Number])
    case ObjectType.DATE     => (writer, elem) => writer.value(DateParsing.formatDate(elem.asInstanceOf[Date]))
    case ObjectType.GEOMETRY => (writer, elem) => writer.jsonValue(geometryWriter.write(elem.asInstanceOf[Geometry]))
    case ObjectType.BYTES    => (writer, elem) => writer.value(Base64.getEncoder.encodeToString(elem.asInstanceOf[Array[Byte]]))
    case ObjectType.BOOLEAN  => (writer, elem) => writer.value(elem.asInstanceOf[java.lang.Boolean])
    case _                   => (writer, elem) => writer.value(elem.toString)
  }
}

