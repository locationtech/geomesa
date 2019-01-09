/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import java.lang.ref.SoftReference

import com.jayway.jsonpath.Option.{ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS}
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.geotools.factory.Hints
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.filter.expression.{PropertyAccessor, PropertyAccessorFactory}
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{PathAttribute, PathAttributeWildCard, PathDeepScan, PathElement}
import org.locationtech.geomesa.features.kryo.json.JsonPathPropertyAccessor.pathFor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.opengis.feature.`type`.AttributeDescriptor
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

  import scala.collection.JavaConverters._

  protected def getFeatureType(obj: AnyRef): SimpleFeatureType
  protected def getValue(descriptor: AttributeDescriptor, attribute: Int, path: Seq[PathElement], obj: AnyRef): Any

  override def canHandle(obj: AnyRef, xpath: String, target: Class[_]): Boolean = {
    val path = try { pathFor(xpath) } catch { case NonFatal(_) => Seq.empty }

    if (path.isEmpty) { false } else {
      path.head match {
        case PathAttribute(name: String, _) =>
          val descriptor = getFeatureType(obj).getDescriptor(name)
          descriptor != null && descriptor.getType.getBinding == classOf[String]
        case PathAttributeWildCard | PathDeepScan =>
          getFeatureType(obj).getAttributeDescriptors.asScala.exists(_.getType.getBinding == classOf[String])
        case _ => false
      }
    }
  }

  override def get[T](obj: AnyRef, xpath: String, target: Class[T]): T = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val path = pathFor(xpath)
    val sft = getFeatureType(obj)

    val attribute = path.head match {
      case PathAttribute(name: String, _) => sft.indexOf(name)
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

    val result = getValue(sft.getDescriptor(attribute), attribute, path.tail, obj)

    if (target == null) {
      result.asInstanceOf[T]
    } else {
      FastConverter.convert(result, target)
    }
  }

  override def set[T](obj: Any, xpath: String, value: T, target: Class[T]): Unit = throw new NotImplementedError()
}

object JsonPathPropertyAccessor {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  // cached references to parsed json path expressions
  private val paths = new java.util.concurrent.ConcurrentHashMap[String, SoftReference[Seq[PathElement]]]()
  private val pathConfig =
    Configuration.builder.options(ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS).build()

  class JsonPropertyAccessorFactory extends PropertyAccessorFactory {

    override def createPropertyAccessor(typ: Class[_],
                                        xpath: String,
                                        target: Class[_],
                                        hints: Hints): PropertyAccessor = {
      if (xpath == null || !xpath.startsWith("$.")) {
        null
      } else if (classOf[SimpleFeature].isAssignableFrom(typ)) {
        JsonPathFeatureAccessor
      } else if (classOf[SimpleFeatureType].isAssignableFrom(typ)) {
        JsonPathTypeAccessor
      } else {
        null
      }
    }
  }

  object JsonPathFeatureAccessor extends JsonPathPropertyAccessor {

    // note: we know object is a simple feature due to checks in factory.createPropertyAccessor

    override protected def getFeatureType(obj: AnyRef): SimpleFeatureType =
      obj.asInstanceOf[SimpleFeature].getFeatureType

    override protected def getValue(descriptor: AttributeDescriptor,
                                    attribute: Int,
                                    path: Seq[PathElement],
                                    obj: AnyRef): Any = {
      obj match {
        case s: KryoBufferSimpleFeature if descriptor.isJson() =>
          KryoJsonSerialization.deserialize(s.getInput(attribute), path)

        case s: SimpleFeature =>
          val json = s.getAttribute(attribute).asInstanceOf[String]
          val list = JsonPath.using(pathConfig).parse(json).read[java.util.List[AnyRef]](JsonPathParser.print(path))
          if (list == null || list.isEmpty) { null } else if (list.size == 1) { list.get(0) } else { list }
      }
    }
  }

  object JsonPathTypeAccessor extends JsonPathPropertyAccessor {

    // note: we know object is a simple feature type due to checks in factory.createPropertyAccessor

    override protected def getFeatureType(obj: AnyRef): SimpleFeatureType = obj.asInstanceOf[SimpleFeatureType]

    override protected def getValue(descriptor: AttributeDescriptor,
                                    attribute: Int,
                                    path: Seq[PathElement],
                                    obj: AnyRef): Any = {
      // remove the json flag, so that transform serializations don't try to serialize
      // json path results that aren't valid json objects or arrays
      val builder = new AttributeTypeBuilder()
      builder.init(descriptor)
      val result = builder.buildDescriptor(descriptor.getLocalName)
      result.getUserData.remove(SimpleFeatureTypes.AttributeOptions.OPT_JSON)
      result
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

