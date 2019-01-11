/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import org.geotools.factory.Hints
import org.geotools.factory.Hints.Key
import org.geotools.util.{Converter, ConverterFactory}

/**
  * Converts between strings and collections (maps and lists).
  *
  * The collection subtype(s) need to be passed into the hints using keys from ConverterFactories.
  * This is required, as we can't use reflection to figure out subtypes due to type erasure.
  * If the subtypes were incorrect, we could get serialization exceptions.
  */
class StringCollectionConverterFactory extends ConverterFactory {

  import StringCollectionConverterFactory._

  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter = {
    if (source != classOf[String] || hints == null || hints.isEmpty) {
      null
    } else if (classOf[java.util.List[_]].isAssignableFrom(target)) {
      val listType = hints.get(ListTypeKey).asInstanceOf[Class[_]]
      if (listType == null) {
        null
      } else {
        new StringToListConverter(listType)
      }
    } else if (classOf[java.util.Map[_, _]].isAssignableFrom(target)) {
      val keyType   = hints.get(MapKeyTypeKey).asInstanceOf[Class[_]]
      val valueType = hints.get(MapValueTypeKey).asInstanceOf[Class[_]]
      if (keyType == null || valueType == null) {
        null
      } else {
        new StringToMapConverter(keyType, valueType)
      }
    } else {
      null
    }
  }
}

object StringCollectionConverterFactory {

  val ListTypeKey     = new Key(classOf[Class[_]])
  val MapKeyTypeKey   = new Key(classOf[Class[_]])
  val MapValueTypeKey = new Key(classOf[Class[_]])

  /**
    * Converts a specially formatted string to a java list.
    *
    * Examples:
    *   "[value1, value2]" // from java List.toString
    *   "value1,value2" // from geomesa feature exporter conversion
    *
    * @param listType type of list elements
    */
  class StringToListConverter(listType: Class[_]) extends Converter {

    override def convert[T](source: scala.Any, target: Class[T]): T = {
      val string = source.toString.trim
      // strip outer designators if any
      // java .toString looks like: [value1, value2]
      // geomesa looks like: value1,value2
      val isToString = string.startsWith("[") && string.endsWith("]")
      val stripped = if (isToString) string.substring(1, string.length - 1) else string
      if (string.isEmpty) {
        new java.util.ArrayList[Any](0).asInstanceOf[T]
      } else {
        val result = new java.util.ArrayList[Any]
        stripped.split(",").foreach { e =>
          val converted = FastConverter.convert(e.trim, listType)
          if (converted != null) {
            result.add(converted)
          }
        }
        if (result.isEmpty) {
          // we failed to convert anything... return null and let someone else try
          null.asInstanceOf[T]
        } else {
          result.asInstanceOf[T]
        }
      }
    }
  }

  /**
    * Converts a specially formatted string to a java map.
    *
    * Examples:
    *   "{key1=value1, key2=value2}" // from java Map.toString
    *   "key1->value1,key2->value2" // from geomesa feature exporter conversion
    *
    * @param keyType map key type
    * @param valueType map value type
    */
  class StringToMapConverter(keyType: Class[_], valueType: Class[_]) extends Converter {

    override def convert[T](source: scala.Any, target: Class[T]): T = {
      val string = source.toString.trim
      // pick our delimiter and strip outer designators if any
      // java .toString looks like: {key1=value1, key2=value2}
      // geomesa looks like: key1->value1,key2->value2
      val isToString = string.startsWith("{") && string.endsWith("}")
      val (stripped, split) = if (isToString) (string.substring(1, string.length - 1), "=") else (string, "->")
      if (stripped.isEmpty) {
        new java.util.HashMap[Any, Any](0).asInstanceOf[T]
      } else {
        val result = new java.util.HashMap[Any, Any]
        stripped.split(",").map(_.split(split)).filter(_.length == 2).foreach { case Array(k, v) =>
          val convertedKey = FastConverter.convert(k.trim, keyType)
          if (convertedKey != null) {
            val convertedValue = FastConverter.convert(v.trim, valueType)
            if (convertedValue != null) {
              result.put(convertedKey, convertedValue)
            }
          }
        }
        if (result.isEmpty) {
          // we failed to convert anything... return null and let someone else try
          null.asInstanceOf[T]
        } else {
          result.asInstanceOf[T]
        }
      }
    }
  }

}