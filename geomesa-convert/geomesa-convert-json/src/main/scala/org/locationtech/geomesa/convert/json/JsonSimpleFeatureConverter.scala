package org.locationtech.geomesa.convert.json

import com.google.gson.{JsonElement, JsonArray}
import com.jayway.jsonpath.spi.json.GsonJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

class JsonSimpleFeatureConverter(jsonConfig: Configuration,
                                 val targetSFT: SimpleFeatureType,
                                 val root: Option[JsonPath],
                                 val inputFields: IndexedSeq[Field],
                                 val idBuilder: Expr) extends ToSimpleFeatureConverter[String] {

  import scala.collection.JavaConversions._

  override def fromInputType(i: String): Seq[Array[Any]] =
     Try { jsonConfig.jsonProvider.parse(i) }.map { json =>
       root.map { r => extractFromRoot(json, r) }.getOrElse(Seq(Array[Any](json)))
     }.getOrElse(Seq(Array()))

  def extractFromRoot(json: AnyRef, r: JsonPath): Seq[Array[Any]] =
    r.read[JsonArray](json, jsonConfig).map { o =>
      Array[Any](o)
    }.toSeq

}

class JsonSimpleFeatureConverterFactory extends SimpleFeatureConverterFactory[String] {

  import scala.collection.JavaConversions._

  private val jsonConfig =
    Configuration.builder().jsonProvider(new GsonJsonProvider).build()

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "json")

  override def buildConverter(targetSFT: SimpleFeatureType, conf: Config): SimpleFeatureConverter[String] = {
    val root = if(conf.hasPath("feature-path")) Some(JsonPath.compile(conf.getString("feature-path"))) else None
    val fields = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))

    new JsonSimpleFeatureConverter(jsonConfig, targetSFT, root, fields, idBuilder)
  }

  override def buildFields(fields: Seq[Config]): IndexedSeq[Field] = {
    fields.map { f =>
      val name = f.getString("name")
      val transform = if (f.hasPath("transform")) {
        Transformers.parseTransform(f.getString("transform"))
      } else {
        null
      }
      if (f.hasPath("path")) {
        // path can be absolute, or relative to the feature node
        // it can also include xpath functions to manipulate the result
        JsonField(name, JsonPath.compile(f.getString("path")), jsonConfig, transform, f.getString("json-type"))
      } else {
        SimpleField(name, transform)
      }
    }.toIndexedSeq
  }

}

object JsonField {
  def apply(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr, jsonType: String) = jsonType match {
    case "string"  => StringJsonField(name, expression, jsonConfig, transform)
    case "double"  => DoubleJsonField(name, expression, jsonConfig, transform)
    case "integer" => IntJsonField(name, expression, jsonConfig, transform)
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

case class DoubleJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr) extends BaseJsonField[java.lang.Double] {
  override def getAs(el: JsonElement): java.lang.Double = el.getAsDouble
}

case class IntJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr) extends BaseJsonField[java.lang.Integer] {
  override def getAs(el: JsonElement): java.lang.Integer = el.getAsInt
}

case class StringJsonField(name: String, expression: JsonPath, jsonConfig: Configuration, transform: Expr) extends BaseJsonField[java.lang.String] {
  override def getAs(el: JsonElement): String = el.getAsString
}

