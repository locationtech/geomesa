/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.io.InputStream
import java.nio.charset.StandardCharsets

import com.google.gson.{JsonArray, JsonElement, JsonObject}
import com.jayway.jsonpath.spi.json.GsonJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.typesafe.config.Config
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.LineMode.LineMode
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.io.Source

class JsonSimpleFeatureConverter(jsonConfig: Configuration,
                                 val targetSFT: SimpleFeatureType,
                                 val root: Option[JsonPath],
                                 val inputFields: IndexedSeq[Field],
                                 val idBuilder: Expr,
                                 val userDataBuilder: Map[String, Expr],
                                 val parseOpts: ConvertParseOpts,
                                 val lineMode: LineMode) extends ToSimpleFeatureConverter[String] {

  import scala.collection.JavaConversions._

  override def fromInputType(i: String): Seq[Array[Any]] = {
    val json = jsonConfig.jsonProvider.parse(i)
    root.map(extractFromRoot(json, _)).getOrElse(Seq(Array[Any](json)))
  }

  // NB:  Currently the JSON support for Converters parses the entire JSON document into memory.
  //  In the event that we wish to build SimpleFeatures from a 'feature' path and the 'root' path, we have a small issue.
  //  This solution involves handing a pointer to the feature path and the entire document.
  //  In the converter config, use 'root-path' to defined paths which reference the entire document.
  private def extractFromRoot(json: AnyRef, r: JsonPath): Seq[Array[Any]] =
    r.read[JsonArray](json, jsonConfig).map(o => Array[Any](o, json)).toSeq

  // TODO GEOMESA-1039 more efficient InputStream processing for multi mode
  override def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature] =
    lineMode match {
      case LineMode.Single =>
        processInput(Source.fromInputStream(is, StandardCharsets.UTF_8.displayName).getLines(), ec)
      case LineMode.Multi =>
        processInput(Iterator(IOUtils.toString(is, StandardCharsets.UTF_8.displayName)), ec)
    }

}

class JsonSimpleFeatureConverterFactory extends AbstractSimpleFeatureConverterFactory[String] {

  private val jsonConfig =
    Configuration.builder()
      .jsonProvider(new GsonJsonProvider)
      .options(com.jayway.jsonpath.Option.DEFAULT_PATH_LEAF_TO_NULL)
      .build()

  override protected val typeToProcess = "json"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        parseOpts: ConvertParseOpts): SimpleFeatureConverter[String] = {
    val lineMode = LineMode.getLineMode(conf)
    val root = if (conf.hasPath("feature-path")) Some(JsonPath.compile(conf.getString("feature-path"))) else None
    new JsonSimpleFeatureConverter(jsonConfig, sft, root, fields, idBuilder, userDataBuilder, parseOpts, lineMode)
  }

  override protected def buildField(field: Config): Field = {
    val name = field.getString("name")
    val transform = if (field.hasPath("transform")) {
      Transformers.parseTransform(field.getString("transform"))
    } else {
      null
    }
    if (field.hasPath("path")) {
      // path can be absolute, or relative to the feature node
      JsonField(name, JsonPath.compile(field.getString("path")), jsonConfig, transform, field.getString("json-type"), false)
    } else if (field.hasPath("root-path")) {
      // when 'root-path' is used, the path is absolute
      JsonField(name, JsonPath.compile(field.getString("root-path")), jsonConfig, transform, field.getString("json-type"), true)
    } else {
      SimpleField(name, transform)
    }
  }
}

object JsonField {
  def apply(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, jsonType: String, pathIsRoot: Boolean) = jsonType match {
    case "string"           => StringJsonField(name, expression, jsonConfig, transform, pathIsRoot)
    case "float"            => FloatJsonField(name, expression, jsonConfig, transform, pathIsRoot)
    case "double"           => DoubleJsonField(name, expression, jsonConfig, transform, pathIsRoot)
    case "int" | "integer"  => IntJsonField(name, expression, jsonConfig, transform, pathIsRoot)
    case "bool" | "boolean" => BooleanJsonField(name, expression, jsonConfig, transform, pathIsRoot)
    case "long"             => LongJsonField(name, expression, jsonConfig, transform, pathIsRoot)
    case "geometry"         => GeometryJsonField(name, expression, jsonConfig, transform, pathIsRoot)
    case "list" | "array"   => JsonArrayField(name, expression, jsonConfig, transform, pathIsRoot)
    case "map" |"object"    => JsonObjectField(name, expression, jsonConfig, transform, pathIsRoot)
  }
}

trait BaseJsonField[T] extends Field {

  def name: String
  def expression: JsonPath
  def jsonConfig: Configuration
  def transform: Expr
  def pathIsRoot: Boolean

  private val mutableArray = Array.ofDim[Any](1)

  def getAs(el: JsonElement): T

  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    mutableArray(0) = getAs(evaluateJsonPath(args))

    if(transform == null) mutableArray(0)
    else super.eval(mutableArray)
  }

  def evalWithTransform(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    mutableArray(0) = evaluateJsonPath(args)
    super.eval(mutableArray)
  }

  // If the expression path is the 'root' path, we read from the second, argument.
  // In order for there to be two arguments passed in, one must be using a feature-path.
  //  Without a feature-path, just use 'path' rather than 'root-path'.
  def evaluateJsonPath(args: Array[Any]): JsonElement = {
    val arg = if (pathIsRoot) args(1) else args(0)
    expression.read[JsonElement](arg, jsonConfig)
  }

}

case class BooleanJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
    extends BaseJsonField[java.lang.Boolean] {
  override def getAs(el: JsonElement): java.lang.Boolean = if (el.isJsonNull) null else el.getAsBoolean
}

case class IntJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
    extends BaseJsonField[java.lang.Integer] {
  override def getAs(el: JsonElement): java.lang.Integer = if (el.isJsonNull) null else el.getAsInt
}

case class LongJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
    extends BaseJsonField[java.lang.Long] {
  override def getAs(el: JsonElement): java.lang.Long = if (el.isJsonNull) null else el.getAsBigInteger.longValue()
}

case class FloatJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
  extends BaseJsonField[java.lang.Float] {
  override def getAs(el: JsonElement): java.lang.Float = if (el.isJsonNull) null else el.getAsFloat
}

case class DoubleJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
    extends BaseJsonField[java.lang.Double] {
  override def getAs(el: JsonElement): java.lang.Double = if (el.isJsonNull) null else el.getAsDouble
}

case class StringJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
    extends BaseJsonField[java.lang.String] {
  override def getAs(el: JsonElement): String = if (el.isJsonNull) null else el.getAsString
}

case class GeometryJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
    extends BaseJsonField[Geometry] with GeoJsonParsing {
  override def getAs(el: JsonElement): Geometry =  parseGeometry(el)
}

case class JsonArrayField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
  extends BaseJsonField[JsonArray] {
  override def getAs(el: JsonElement): JsonArray = if (el.isJsonNull) null else el.getAsJsonArray
}

case class JsonObjectField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, pathIsRoot: Boolean)
  extends BaseJsonField[JsonObject] {
  override def getAs(el: JsonElement): JsonObject = if (el.isJsonNull) null else el.getAsJsonObject

}

trait GeoJsonParsing {
  val geoFac = new GeometryFactory

  def toPointCoords(el: JsonElement): Coordinate = {
    val arr = el.getAsJsonArray.iterator.map(_.getAsDouble).toArray
    new Coordinate(arr(0), arr(1))
  }

  def toCoordSeq(el: JsonElement): CoordinateSequence = {
    val arr = el.getAsJsonArray.iterator.map(_.getAsJsonArray).map(toPointCoords).toArray
    new CoordinateArraySequence(arr)
  }

  private val CoordsPath = "coordinates"
  def parseGeometry(el: JsonElement): Geometry = {
    if (el.isJsonObject) {
      val geomType = el.getAsJsonObject.get("type").getAsString.toLowerCase
      geomType match {
        case "point" =>
          geoFac.createPoint(toPointCoords(el.getAsJsonObject.get(CoordsPath)))
        case "linestring" =>
          geoFac.createLineString(toCoordSeq(el.getAsJsonObject.get(CoordsPath)))
        case "polygon" =>
          // Only simple polygons for now (one linear ring)
          val coords = el.getAsJsonObject.get(CoordsPath).getAsJsonArray.iterator.map(toCoordSeq).toArray
          geoFac.createPolygon(coords(0))
      }
    } else if (el.isJsonNull) {
      null.asInstanceOf[Geometry]
    } else if (el.isJsonPrimitive) {
       WKTUtils.read(el.getAsString)
    } else {
      throw new IllegalArgumentException(s"Unknown geometry type: $el")
    }
  }
}