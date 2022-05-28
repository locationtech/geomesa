/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.io._
import java.nio.charset.Charset

import com.google.gson._
import com.google.gson.stream.{JsonReader, JsonToken}
import com.jayway.jsonpath.spi.json.GsonJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath, PathNotFoundException}
import com.typesafe.config.Config
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.json.JsonConverter._
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig, Field}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType

class JsonConverter(sft: SimpleFeatureType, config: JsonConfig, fields: Seq[JsonField], options: BasicOptions)
    extends AbstractConverter[JsonElement, JsonConfig, JsonField, BasicOptions](sft, config, fields, options) {

  import scala.collection.JavaConverters._

  private val featurePath = config.featurePath.map(JsonPath.compile(_))

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[JsonElement] =
    new JsonIterator(is, options.encoding, ec)

  override protected def values(parsed: CloseableIterator[JsonElement],
                                ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    val array = Array.ofDim[Any](2)
    featurePath match {
      case None =>
        parsed.map { element =>
          array(0) = element
          array
        }
      case Some(path) =>
        parsed.flatMap { element =>
          array(1) = element
          path.read[JsonArray](element, JsonConfiguration).iterator.asScala.map { e =>
            array(0) = e
            array
          }
        }
    }
  }
}

object JsonConverter extends GeoJsonParsing {

  private [json] val JsonConfiguration =
    Configuration.builder()
        .jsonProvider(new GsonJsonProvider)
        .options(com.jayway.jsonpath.Option.DEFAULT_PATH_LEAF_TO_NULL)
        .build()

  private val LineRegex = """JsonReader at line (\d+)""".r

  case class JsonConfig(
      `type`: String,
      featurePath: Option[String],
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig

  sealed trait JsonField extends Field

  case class DerivedField(name: String, transforms: Option[Expression]) extends JsonField {
    override val fieldArg: Option[Array[AnyRef] => AnyRef] = None
  }

  abstract class TypedJsonField(val jsonType: String) extends JsonField {

    def path: String
    def pathIsRoot: Boolean

    private val i = if (pathIsRoot) { 1 } else { 0 }
    private val jsonPath = JsonPath.compile(path)

    protected def unwrap(elem: JsonElement): AnyRef

    override val fieldArg: Option[Array[AnyRef] => AnyRef] = Some(values)

    private def values(args: Array[AnyRef]): AnyRef = {
      val e = try { jsonPath.read[JsonElement](args(i), JsonConfiguration) } catch {
        case _: PathNotFoundException => JsonNull.INSTANCE
      }
      if (e.isJsonNull) { null } else { unwrap(e) }
    }
  }

  case class StringJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("string") {
    override def unwrap(elem: JsonElement): AnyRef = elem.getAsString
  }

  case class FloatJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("float") {
    override def unwrap(elem: JsonElement): AnyRef = Float.box(elem.getAsFloat)
  }

  case class DoubleJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("double") {
    override def unwrap(elem: JsonElement): AnyRef = Double.box(elem.getAsDouble)
  }

  case class IntJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("int") {
    override def unwrap(elem: JsonElement): AnyRef = Int.box(elem.getAsInt)
  }

  case class BooleanJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("boolean") {
    override def unwrap(elem: JsonElement): AnyRef = Boolean.box(elem.getAsBoolean)
  }

  case class LongJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("long") {
    override def unwrap(elem: JsonElement): AnyRef = Long.box(elem.getAsBigInteger.longValue())
  }

  case class GeometryJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("geometry") {
    override def unwrap(elem: JsonElement): AnyRef = parseGeometry(elem)
  }

  case class ArrayJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("array") {
    override def unwrap(elem: JsonElement): AnyRef = elem.getAsJsonArray
  }

  case class ObjectJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField("object") {
    override def unwrap(elem: JsonElement): AnyRef = elem.getAsJsonObject
  }

  /**
    * Parses an input stream into json elements
    *
    * @param is input
    * @param encoding encoding
    * @param ec context
    */
  class JsonIterator private [json] (is: InputStream, encoding: Charset, ec: EvaluationContext)
      extends CloseableIterator[JsonElement] {

    private val parser = new JsonParser()
    private val reader = new JsonReader(new InputStreamReader(is, encoding))
    reader.setLenient(true)

    override def hasNext: Boolean = reader.peek() != JsonToken.END_DOCUMENT
    override def next(): JsonElement = {
      val res = parser.parse(reader)
      // extract the line number, only accessible from reader.toString
      LineRegex.findFirstMatchIn(reader.toString).foreach(m => ec.line = m.group(1).toLong)
      res
    }

    override def close(): Unit = reader.close()
  }
}
