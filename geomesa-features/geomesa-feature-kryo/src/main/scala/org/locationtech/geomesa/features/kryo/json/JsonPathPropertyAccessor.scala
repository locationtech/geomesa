/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.Option.{ALWAYS_RETURN_LIST, SUPPRESS_EXCEPTIONS}
import net.minidev.json.JSONObject
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.filter.expression.{PropertyAccessor, PropertyAccessorFactory}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.json.JsonPathParser._
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter

import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/**
  * Access values from a json-type string field. Syntax must start with '$.'. The first part of the path
  * selects the simple feature attribute, and the rest of the path selects within the json contained in
  * that attribute.
  *
  * Note: this class is optimized for `KryoBufferSimpleFeature`s. It will work on standard simple features,
  * but will incur a serialization cost.
  */
trait JsonPathPropertyAccessor extends PropertyAccessor {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  override def canHandle(obj: AnyRef, xpath: String, target: Class[_]): Boolean = {
    val path = try { JsonPathPropertyAccessor.paths.get(xpath) } catch { case NonFatal(e) => e.printStackTrace; JsonPath(Seq.empty, None) }

    if (path.isEmpty) { false } else {
      path.head match {
        case PathAttribute(name: String, _) =>
          val descriptor = obj match {
            case s: SimpleFeature => s.getFeatureType.getDescriptor(name)
            case s: SimpleFeatureType => s.getDescriptor(name)
            case _ => null
          }
          descriptor != null && descriptor.getType.getBinding == classOf[String]

        case PathAttributeWildCard | PathDeepScan =>
          val sft = obj match {
            case s: SimpleFeature => s.getFeatureType
            case s: SimpleFeatureType => s
            case _ => null
          }
          sft != null && sft.getAttributeDescriptors.asScala.exists(_.getType.getBinding == classOf[String])

        case _ => false
      }
    }
  }

  override def get[T](obj: AnyRef, xpath: String, target: Class[T]): T = {

    val path = JsonPathPropertyAccessor.paths.get(xpath)

    val result = obj match {
      case s: KryoBufferSimpleFeature =>
        val i = attribute(s.getFeatureType, path.head)
        if (s.getFeatureType.getDescriptor(i).isJson()) {
          s.getInput(i).map(KryoJsonSerialization.deserialize(_, path.tail)).orNull
        } else {
          JsonPathPropertyAccessor.evaluateJsonPath(s.getAttribute(i).asInstanceOf[String], path.tail)
        }

      case s: SimpleFeature =>
        JsonPathPropertyAccessor.evaluateJsonPath(s.getAttribute(attribute(s.getFeatureType, path.head)).asInstanceOf[String], path.tail)

      case s: SimpleFeatureType =>
        // remove the json flag, so that transform serializations don't try to serialize
        // json path results that aren't valid json objects or arrays
        val descriptor = s.getDescriptor(attribute(s, path.head))
        val builder = new AttributeTypeBuilder()
        builder.init(descriptor)
        val result = builder.buildDescriptor(descriptor.getLocalName)
        result.getUserData.remove(SimpleFeatureTypes.AttributeOptions.OptJson)
        result
    }

    if (target == null) {
      result.asInstanceOf[T]
    } else {
      FastConverter.convert(result, target)
    }
  }

  override def set[T](obj: Any, xpath: String, value: T, target: Class[T]): Unit = throw new NotImplementedError()

  private def attribute(sft: SimpleFeatureType, head: PathElement): Int = {
    head match {
      case PathAttribute(name: String, _) =>
        sft.indexOf(name)

      case _ =>
        // we know it will be a wildcard due to canHandle
        // prioritize fields marked json over generic strings
        // note: will only match first json attribute if more than 1
        val i = sft.getAttributeDescriptors.asScala.indexWhere(_.isJson())
        if (i != -1) { i } else {
          sft.getAttributeDescriptors.asScala.indexWhere(_.getType.getBinding == classOf[String])
        }
    }
  }
}

object JsonPathPropertyAccessor extends JsonPathPropertyAccessor {

  import scala.collection.JavaConverters._

  val CacheExpiry: SystemProperty = SystemProperty("geomesa.json.cache.expiry", "10 minutes")

  // cached references to parsed json path expressions
  private val paths: LoadingCache[String, JsonPath] =
    Caffeine.newBuilder()
      .expireAfterAccess(CacheExpiry.toDuration.get.toMillis, TimeUnit.MILLISECONDS)
      .build(new CacheLoader[String, JsonPath]() {
        override def load(path: String): JsonPath = JsonPathParser.parse(path, report = false)
      })

  private val pathConfig: Configuration = Configuration.builder.options(ALWAYS_RETURN_LIST, SUPPRESS_EXCEPTIONS).build()

  class JsonPropertyAccessorFactory extends PropertyAccessorFactory {

    override def createPropertyAccessor(
        typ: Class[_],
        xpath: String,
        target: Class[_],
        hints: Hints): PropertyAccessor = {
      if (xpath != null &&
          xpath.startsWith("$.") &&
          (classOf[SimpleFeature].isAssignableFrom(typ) || classOf[SimpleFeatureType].isAssignableFrom(typ))) {
        JsonPathPropertyAccessor
      } else {
        null
      }
    }
  }

  /**
   * Evaluate a json path using the json-path lib. This method is a fallback for our custom kryo-deserialization,
   * and requires parsing the input into the json-path AST. The result has been modified to be consistent
   * with our custom serialization (for better interop with geotools), i.e. json objects are serialized to json
   * strings.
   *
   * @param json json doc
   * @param path json path
   * @return
   */
  private[json] def evaluateJsonPath(json: String, path: JsonPath): AnyRef = {
    val parsed = com.jayway.jsonpath.JsonPath.using(pathConfig).parse(json)
    val list = parsed.read[java.util.List[AnyRef]](JsonPathParser.print(path))
    if (list == null || list.isEmpty) {
      // special handling to return 0 for length functions without matches
      path.function match {
        case Some(PathFunction.LengthFunction) => Int.box(0)
        case _ => null
      }
    } else {
      val transformed = list.asScala.map {
        case o: java.util.Map[String, AnyRef] => JSONObject.toJSONString(o)
        case a: java.util.List[AnyRef] => unwrapArray(a)
        case p => p
      }
      if (transformed.lengthCompare(1) == 0) { transformed.head } else { transformed.asJava }
    }
  }

  private def unwrapArray(array: java.util.List[AnyRef]): java.util.List[AnyRef] = {
    array.asScala.map {
      case o: java.util.Map[String, AnyRef] => JSONObject.toJSONString(o)
      case a: java.util.List[AnyRef] => unwrapArray(a)
      case p => p
    }.asJava
  }
}

