/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo.json

import java.lang.ref.SoftReference

import com.jayway.jsonpath.Option.{ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS}
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.geotools.factory.Hints
import org.geotools.filter.expression.{PropertyAccessor, PropertyAccessorFactory}
import org.geotools.util.Converters
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{PathAttribute, PathAttributeWildCard, PathDeepScan, PathElement}
import org.opengis.feature.simple.SimpleFeature

import scala.util.control.NonFatal

/**
  * Access values from a json-type string field. Syntax must start with '$.'. The first part of the path
  * selects the simple feature attribute, and the rest of the path selects within the json contained in
  * that attribute.
  *
  * Note: this class is optimized for `KryoBufferSimpleFeature`s. It will work on standard simple features,
  * but will incur a serialization cost.
  */
object JsonPathPropertyAccessor extends PropertyAccessor {

  // cached references to parsed json path expressions
  private val paths = new java.util.concurrent.ConcurrentHashMap[String, SoftReference[Seq[PathElement]]]()
  private val pathConfig =
    Configuration.builder.options(ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS).build()

  override def canHandle(obj: Any, xpath: String, target: Class[_]): Boolean = {
    val path = try { pathFor(xpath) } catch { case NonFatal(e) => Seq.empty }

    if (path.isEmpty) { false } else {
      // we know object is a simple feature due to factory method `createPropertyAccessor`
      val sft = obj.asInstanceOf[SimpleFeature].getFeatureType
      path.head match {
        case PathAttribute(name: String) =>
          val descriptor = sft.getDescriptor(name)
          descriptor != null && descriptor.getType.getBinding == classOf[String]
        case PathAttributeWildCard | PathDeepScan =>
          import scala.collection.JavaConversions._
          sft.getAttributeDescriptors.exists(_.getType.getBinding == classOf[String])
        case _ => false
      }
    }
  }

  override def get[T](obj: Any, xpath: String, target: Class[T]): T = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val sft = obj.asInstanceOf[SimpleFeature].getFeatureType
    val path = pathFor(xpath)

    val attribute = path.head match {
      case PathAttribute(name: String) => sft.indexOf(name)
      case _ =>
        // we know it will be a wildcard due to canHandle
        // prioritize fields marked json over generic strings
        // note: will only match first json attribute if more than 1
        import scala.collection.JavaConversions._
        val i = sft.getAttributeDescriptors.indexWhere(_.isJson())
        if (i != -1) { i } else {
          sft.getAttributeDescriptors.indexWhere(_.getType.getBinding == classOf[String])
        }
    }

    val result = if (sft.getDescriptor(attribute).isJson() && obj.isInstanceOf[KryoBufferSimpleFeature]) {
      val input = obj.asInstanceOf[KryoBufferSimpleFeature].getInput(attribute)
      KryoJsonSerialization.deserialize(input, path.tail)
    } else {
      val json = obj.asInstanceOf[SimpleFeature].getAttribute(attribute).asInstanceOf[String]
      val list = JsonPath.using(pathConfig).parse(json).read[java.util.List[AnyRef]](JsonPathParser.print(path.tail))
      if (list == null || list.isEmpty) { null } else if (list.size == 1) { list.get(0) } else { list }
    }

    if (target == null) {
      result.asInstanceOf[T]
    } else {
      Converters.convert(result, target)
    }
  }

  override def set[T](obj: Any, xpath: String, value: T, target: Class[T]): Unit = throw new NotImplementedError()

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

class JsonPropertyAccessorFactory extends PropertyAccessorFactory {

  override def createPropertyAccessor(typ: Class[_],
                                      xpath: String,
                                      target: Class[_],
                                      hints: Hints): PropertyAccessor = {
    if (classOf[SimpleFeature].isAssignableFrom(typ) && xpath != null && xpath.startsWith("$.")) {
      JsonPathPropertyAccessor
    } else {
      null
    }
  }
}
