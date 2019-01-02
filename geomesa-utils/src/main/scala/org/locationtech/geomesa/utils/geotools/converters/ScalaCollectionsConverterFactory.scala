/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import org.geotools.factory.Hints
import org.geotools.util.{Converter, ConverterFactory}

class ScalaCollectionsConverterFactory extends ConverterFactory {

  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter =
    if (classOf[Seq[_]].isAssignableFrom(source)
        && classOf[java.util.List[_]].isAssignableFrom(target)) {
      ScalaCollectionsConverterFactory.ListToJavaList
    } else if (classOf[java.util.List[_]].isAssignableFrom(source)
        && classOf[Seq[_]].isAssignableFrom(target)) {
      ScalaCollectionsConverterFactory.JavaListToList
    } else if (classOf[Map[_, _]].isAssignableFrom(source)
        && classOf[java.util.Map[_, _]].isAssignableFrom(target)) {
      ScalaCollectionsConverterFactory.MapToJavaMap
    } else if (classOf[java.util.Map[_, _]].isAssignableFrom(source)
        && classOf[Map[_, _]].isAssignableFrom(target)) {
      ScalaCollectionsConverterFactory.JavaMapToMap
    } else {
      null
    }
}

object ScalaCollectionsConverterFactory {

  private val ListToJavaList = new ListToListConverter(true)
  private val JavaListToList = new ListToListConverter(false)
  private val MapToJavaMap   = new MapToMapConverter(true)
  private val JavaMapToMap   = new MapToMapConverter(false)

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

}