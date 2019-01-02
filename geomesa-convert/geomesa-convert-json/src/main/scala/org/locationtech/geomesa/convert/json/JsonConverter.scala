/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.io._

import com.google.gson._
import com.google.gson.stream.{JsonReader, JsonToken}
import com.jayway.jsonpath.spi.json.GsonJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath, PathNotFoundException}
import com.typesafe.config.Config
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.json.JsonConverter.{JsonConfig, JsonField}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig, Field}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class JsonConverter(targetSft: SimpleFeatureType, config: JsonConfig, fields: Seq[JsonField], options: BasicOptions)
    extends AbstractConverter(targetSft, config, fields, options) {

  private val parser = new JsonParser
  private val featurePath = config.featurePath.map(JsonPath.compile(_))

  override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    val reader = new JsonReader(new InputStreamReader(is, options.encoding))
    reader.setLenient(true)

    val elements = new Iterator[JsonElement] {
      override def hasNext: Boolean = reader.peek() != JsonToken.END_DOCUMENT
      override def next(): JsonElement = {
        val res = parser.parse(reader)
        // extract the line number, only accessible from reader.toString
        JsonConverter.lineRegex.findFirstMatchIn(reader.toString).foreach { m =>
          ec.counter.setLineCount(m.group(1).toLong)
        }
        res
      }
    }

    // NB:  Currently the JSON support for Converters parses the entire JSON document into memory.
    //  In the event that we wish to build SimpleFeatures from a 'feature' path and the 'root' path, we have a small issue.
    //  This solution involves handing a pointer to the feature path and the entire document.
    //  In the converter config, use 'root-path' to defined paths which reference the entire document.

    val records = featurePath match {
      case None =>
        elements.map(Array[Any](_))

      case Some(path) =>
        elements.flatMap { element =>
          path.read[JsonArray](element, JsonConverter.jsonConfiguration).map(o => Array[Any](o, element)).iterator
        }
    }

    CloseableIterator(records, reader.close())
  }
}

object JsonConverter extends GeoJsonParsing {

  private val jsonConfiguration =
    Configuration.builder()
        .jsonProvider(new GsonJsonProvider)
        .options(com.jayway.jsonpath.Option.DEFAULT_PATH_LEAF_TO_NULL)
        .build()

  private val lineRegex = """JsonReader at line (\d+)""".r

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
      val e = try { jsonPath.read[JsonElement](args(i), jsonConfiguration) } catch {
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
}
