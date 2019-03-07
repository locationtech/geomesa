/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
    new JsonIterator(is, options.encoding, ec.counter)

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

  case class JsonConfig(`type`: String,
                        featurePath: Option[String],
                        idField: Option[Expression],
                        caches: Map[String, Config],
                        userData: Map[String, Expression]) extends ConverterConfig

  sealed trait JsonField extends Field

  case class DerivedField(name: String, transforms: Option[Expression]) extends JsonField

  abstract class TypedJsonField(val name: String,
                                val jsonType: String,
                                val path: String,
                                val pathIsRoot: Boolean,
                                val transforms: Option[Expression]) extends JsonField {

    private val i = if (pathIsRoot) { 1 } else { 0 }
    private val mutableArray = Array.ofDim[Any](1)
    private val jsonPath = JsonPath.compile(path)

    protected def unwrap(elem: JsonElement): AnyRef

    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
      val e = try { jsonPath.read[JsonElement](args(i), JsonConfiguration) } catch {
        case _: PathNotFoundException => JsonNull.INSTANCE
      }
      mutableArray(0) = if (e.isJsonNull) { null } else { unwrap(e) }
      super.eval(mutableArray)
    }
  }

  class StringJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "string", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = elem.getAsString
  }

  class FloatJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "float", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = Float.box(elem.getAsFloat)
  }

  class DoubleJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "double", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = Double.box(elem.getAsDouble)
  }

  class IntJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "int", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = Int.box(elem.getAsInt)
  }

  class BooleanJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "boolean", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = Boolean.box(elem.getAsBoolean)
  }

  class LongJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "long", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = Long.box(elem.getAsBigInteger.longValue())
  }

  class GeometryJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "geometry", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = parseGeometry(elem)
  }

  class ArrayJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "array", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = elem.getAsJsonArray
  }

  class ObjectJsonField(name: String, path: String, pathIsRoot: Boolean, transforms: Option[Expression])
      extends TypedJsonField(name, "object", path, pathIsRoot, transforms) {
    override def unwrap(elem: JsonElement): AnyRef = elem.getAsJsonObject
  }

  /**
    * Parses an input stream into json elements
    *
    * @param is input
    * @param encoding encoding
    * @param counter counter
    */
  class JsonIterator private [json] (is: InputStream, encoding: Charset, counter: Counter)
      extends CloseableIterator[JsonElement] {

    private val parser = new JsonParser()
    private val reader = new JsonReader(new InputStreamReader(is, encoding))
    reader.setLenient(true)

    override def hasNext: Boolean = reader.peek() != JsonToken.END_DOCUMENT
    override def next(): JsonElement = {
      val res = parser.parse(reader)
      // extract the line number, only accessible from reader.toString
      LineRegex.findFirstMatchIn(reader.toString).foreach(m => counter.setLineCount(m.group(1).toLong))
      res
    }

    override def close(): Unit = reader.close()
  }
}
