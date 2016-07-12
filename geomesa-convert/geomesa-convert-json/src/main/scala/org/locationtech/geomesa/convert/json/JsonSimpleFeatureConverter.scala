/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.io.Source
import scala.util.Try

class JsonSimpleFeatureConverter(jsonConfig: Configuration,
                                 val targetSFT: SimpleFeatureType,
                                 val root: Option[JsonPath],
                                 val inputFields: IndexedSeq[Field],
                                 val idBuilder: Expr,
                                 val userDataBuilder: Map[String, Expr],
                                 val validating: Boolean,
                                 val lineMode: LineMode) extends ToSimpleFeatureConverter[String] {

  import scala.collection.JavaConversions._

  override def fromInputType(i: String): Seq[Array[Any]] =
     Try { jsonConfig.jsonProvider.parse(i) }.map { json =>
       root.map { r => extractFromRoot(json, r) }.getOrElse(Seq(Array[Any](json)))
     }.getOrElse(Seq(Array()))

  def extractFromRoot(json: AnyRef, r: JsonPath): Seq[Array[Any]] =
    r.read[JsonArray](json, jsonConfig).map { o =>
      Array[Any](o)
    }.toSeq

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
                                        validating: Boolean): SimpleFeatureConverter[String] = {
    val lineMode = LineMode.getLineMode(conf)
    val root = if (conf.hasPath("feature-path")) Some(JsonPath.compile(conf.getString("feature-path"))) else None
    new JsonSimpleFeatureConverter(jsonConfig, sft, root, fields, idBuilder, userDataBuilder, validating, lineMode)
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
      // it can also include xpath functions to manipulate the result
      JsonField(name, JsonPath.compile(field.getString("path")), jsonConfig, transform, field.getString("json-type"))
    } else {
      SimpleField(name, transform)
    }
  }
}

object JsonField {
  def apply(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, jsonType: String) = jsonType match {
    case "string"           => StringJsonField(name, expression, jsonConfig, transform)
    case "double"           => DoubleJsonField(name, expression, jsonConfig, transform)
    case "int" | "integer"  => IntJsonField(name, expression, jsonConfig, transform)
    case "bool" | "boolean" => BooleanJsonField(name, expression, jsonConfig, transform)
    case "long"             => LongJsonField(name, expression, jsonConfig, transform)
    case "geometry"         => GeometryJsonField(name, expression, jsonConfig, transform)
    case "list" | "array"   => JsonArrayField(name, expression, jsonConfig, transform)
    case "map" |"object"    => JsonObjectField(name, expression, jsonConfig, transform)
  }
}

trait BaseJsonField[T] extends Field {

  def name: String
  def expression: JsonPath
  def jsonConfig: Configuration
  def transform: Expr

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

  def evaluateJsonPath(args: Array[Any]): JsonElement = expression.read[JsonElement](args(0), jsonConfig)

}

case class BooleanJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
    extends BaseJsonField[java.lang.Boolean] {
  override def getAs(el: JsonElement): java.lang.Boolean = if (el.isJsonNull) null else el.getAsBoolean
}

case class IntJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
    extends BaseJsonField[java.lang.Integer] {
  override def getAs(el: JsonElement): java.lang.Integer = if (el.isJsonNull) null else el.getAsInt
}

case class LongJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
    extends BaseJsonField[java.lang.Long] {
  override def getAs(el: JsonElement): java.lang.Long = if (el.isJsonNull) null else el.getAsBigInteger.longValue()
}

case class DoubleJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
    extends BaseJsonField[java.lang.Double] {
  override def getAs(el: JsonElement): java.lang.Double = if (el.isJsonNull) null else el.getAsDouble
}

case class StringJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
    extends BaseJsonField[java.lang.String] {
  override def getAs(el: JsonElement): String = if (el.isJsonNull) null else el.getAsString
}

case class GeometryJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
    extends BaseJsonField[Geometry] with GeoJsonParsing {
  override def getAs(el: JsonElement): Geometry =  parseGeometry(el)
}

case class JsonArrayField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
  extends BaseJsonField[JsonArray] {
  override def getAs(el: JsonElement): JsonArray = if (el.isJsonNull) null else el.getAsJsonArray
}

case class JsonObjectField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr)
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