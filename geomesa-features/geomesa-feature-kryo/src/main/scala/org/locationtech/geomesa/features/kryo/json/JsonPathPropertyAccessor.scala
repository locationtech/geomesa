/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import java.lang.ref.SoftReference

import com.jayway.jsonpath.Option.{ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS}
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.filter.expression.{PropertyAccessor, PropertyAccessorFactory}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{PathAttribute, PathAttributeWildCard, PathDeepScan, PathElement}
import org.locationtech.geomesa.features.kryo.json.JsonPathPropertyAccessor.{pathConfig, pathFor}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
    val path = try { pathFor(xpath) } catch { case NonFatal(_) => Seq.empty }

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

    val path = pathFor(xpath)

    val result = obj match {
      case s: KryoBufferSimpleFeature =>
        val i = attribute(s.getFeatureType, path.head)
        if (s.getFeatureType.getDescriptor(i).isJson()) {
          s.getInput(i).map(KryoJsonSerialization.deserialize(_, path.tail)).orNull
        } else {
          parse(s.getAttribute(i).asInstanceOf[String], path.tail)
        }

      case s: SimpleFeature =>
        parse(s.getAttribute(attribute(s.getFeatureType, path.head)).asInstanceOf[String], path.tail)

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

  private def parse(json: String, path: Seq[PathElement]): AnyRef = {
    val list = JsonPath.using(pathConfig).parse(json).read[java.util.List[AnyRef]](JsonPathParser.print(path))
    if (list == null || list.isEmpty) { null } else if (list.size == 1) { list.get(0) } else { list }
  }
}

object JsonPathPropertyAccessor extends JsonPathPropertyAccessor {

  // cached references to parsed json path expressions
  private val paths = new java.util.concurrent.ConcurrentHashMap[String, SoftReference[Seq[PathElement]]]()
  private val pathConfig =
    Configuration.builder.options(ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS).build()

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
    * Gets a parsed json path expression, using a cached value if available
    *
    * @param path path to parse
    * @return
    */
  private def pathFor(path: String): Seq[PathElement] = {
    val cached = paths.get(path) match {
      case null => null
      case c => c.get
    }
    if (cached != null) { cached } else {
      val parsed = JsonPathParser.parse(path, report = false)
      paths.put(path, new SoftReference(parsed))
      parsed
    }
  }
}

