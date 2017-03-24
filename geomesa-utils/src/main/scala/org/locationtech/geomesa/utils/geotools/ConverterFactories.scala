/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.factory.Hints
import org.geotools.factory.Hints.Key
import org.geotools.util.{Converter, ConverterFactory, Converters}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object ConverterFactories {
  val ListTypeKey     = new Key(classOf[Class[_]])
  val MapKeyTypeKey   = new Key(classOf[Class[_]])
  val MapValueTypeKey = new Key(classOf[Class[_]])
}

class JodaConverterFactory extends ConverterFactory {

  private val printer = ISODateTimeFormat.dateTime().withZoneUTC()
  private val parser  = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  def createConverter(source: Class[_], target: Class[_], hints: Hints) =
    if(classOf[java.util.Date].isAssignableFrom(source) && classOf[String].isAssignableFrom(target)) {
      // Date => String
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          printer.print(new DateTime(source.asInstanceOf[java.util.Date])).asInstanceOf[T]
      }
    } else if(classOf[java.util.Date].isAssignableFrom(target) && classOf[String].isAssignableFrom(source)) {
      // String => Date
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          parser.parseDateTime(source.asInstanceOf[String]).toDate.asInstanceOf[T]
      }
    } else null.asInstanceOf[Converter]
}

class ScalaCollectionsConverterFactory extends ConverterFactory {

  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter =
    if (classOf[Seq[_]].isAssignableFrom(source)
        && classOf[java.util.List[_]].isAssignableFrom(target)) {
      new ListToListConverter(true)
    } else if (classOf[java.util.List[_]].isAssignableFrom(source)
        && classOf[Seq[_]].isAssignableFrom(target)) {
      new ListToListConverter(false)
    } else if (classOf[Map[_, _]].isAssignableFrom(source)
        && classOf[java.util.Map[_, _]].isAssignableFrom(target)) {
      new MapToMapConverter(true)
    } else if (classOf[java.util.Map[_, _]].isAssignableFrom(source)
        && classOf[Map[_, _]].isAssignableFrom(target)) {
      new MapToMapConverter(false)
    } else {
      null
    }
}

/**
  * Converts between strings and collections (maps and lists).
  *
  * The collection subtype(s) need to be passed into the hints using keys from ConverterFactories.
  * This is required, as we can't use reflection to figure out subtypes due to type erasure.
  * If the subtypes were incorrect, we could get serialization exceptions.
  */
class StringCollectionConverterFactory extends ConverterFactory {

  import ConverterFactories.{ListTypeKey, MapKeyTypeKey, MapValueTypeKey}

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
        val converted = Converters.convert(e.trim, listType)
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
        val convertedKey = Converters.convert(k.trim, keyType)
        if (convertedKey != null) {
          val convertedValue = Converters.convert(v.trim, valueType)
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

/**
  * Convert between scala and java lists
  *
  * @param scalaToJava scala to java or java to scala
  */
class ListToListConverter(scalaToJava: Boolean) extends Converter {

  import scala.collection.JavaConverters._

  override def convert[T](source: scala.Any, target: Class[T]): T = {
    if (scalaToJava) {
      source.asInstanceOf[Seq[_]].asJava.asInstanceOf[T]
    } else {
      source.asInstanceOf[java.util.List[_]].asScala.asInstanceOf[T]
    }
  }
}

/**
  * Convert between scala and java maps
  *
  * @param scalaToJava scala to java or java to scala
  */
class MapToMapConverter(scalaToJava: Boolean) extends Converter {

  import scala.collection.JavaConverters._

  override def convert[T](source: scala.Any, target: Class[T]): T = {
    if (scalaToJava) {
      source.asInstanceOf[Map[_, _]].asJava.asInstanceOf[T]
    } else {
      source.asInstanceOf[java.util.Map[_, _]].asScala.asInstanceOf[T]
    }
  }
}
