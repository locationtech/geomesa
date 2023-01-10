/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import org.geotools.util.factory.Hints
import org.geotools.util.{Converter, ConverterFactory}

class ScalaCollectionsConverterFactory extends ConverterFactory {

  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter = {
    if (classOf[java.util.List[_]].isAssignableFrom(target)) {
       if (classOf[scala.collection.Seq[_]].isAssignableFrom(source)) {
         return ScalaCollectionsConverterFactory.SeqToJava
       }
    } else if (classOf[java.util.List[_]].isAssignableFrom(source)) {
      if (classOf[scala.collection.immutable.Seq[_]].isAssignableFrom(target)) {
        return ScalaCollectionsConverterFactory.JavaToImmutableSeq
      } else if (classOf[scala.collection.mutable.Seq[_]].isAssignableFrom(target)) {
        return ScalaCollectionsConverterFactory.JavaToMutableSeq
      }
    } else if (classOf[java.util.Map[_, _]].isAssignableFrom(target)) {
      if (classOf[scala.collection.Map[_, _]].isAssignableFrom(source)) {
        return ScalaCollectionsConverterFactory.MapToJava
      }
    } else if (classOf[java.util.Map[_, _]].isAssignableFrom(source)) {
      if (classOf[scala.collection.immutable.Map[_, _]].isAssignableFrom(target)) {
        return ScalaCollectionsConverterFactory.JavaToImmutableMap
      } else if (classOf[scala.collection.mutable.Map[_, _]].isAssignableFrom(target)) {
        return ScalaCollectionsConverterFactory.JavaToMutableMap
      }
    }
    null
  }
}

object ScalaCollectionsConverterFactory {

  import scala.collection.JavaConverters._

  object SeqToJava extends Converter {
    override def convert[T](source: Any, target: Class[T]): T =
      source.asInstanceOf[scala.collection.Seq[_]].asJava.asInstanceOf[T]
  }

  object JavaToImmutableSeq extends Converter {
    override def convert[T](source: Any, target: Class[T]): T =
      source.asInstanceOf[java.util.List[_]].asScala.toSeq.asInstanceOf[T]
  }

  object JavaToMutableSeq extends Converter {
    override def convert[T](source: Any, target: Class[T]): T =
      source.asInstanceOf[java.util.List[_]].asScala.asInstanceOf[T]
  }

  object MapToJava extends Converter {
    override def convert[T](source: Any, target: Class[T]): T =
      source.asInstanceOf[scala.collection.Map[_, _]].asJava.asInstanceOf[T]
  }

  object JavaToImmutableMap extends Converter {
    override def convert[T](source: Any, target: Class[T]): T =
      source.asInstanceOf[java.util.Map[_, _]].asScala.toMap.asInstanceOf[T]
  }

  object JavaToMutableMap extends Converter {
    override def convert[T](source: Any, target: Class[T]): T =
      source.asInstanceOf[java.util.Map[_, _]].asScala.asInstanceOf[T]
  }
}