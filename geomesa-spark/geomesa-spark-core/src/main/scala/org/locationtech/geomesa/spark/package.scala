/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.opengis.feature.simple.SimpleFeatureType

package object spark {

  trait Schema {
    def schema: SimpleFeatureType
  }

  // Resolve issue with wrapped instance of org.apache.spark.sql.execution.datasources.CaseInsensitiveMap in Scala 2.10
  object CaseInsensitiveMapFix {
    import scala.collection.convert.Wrappers._

    trait MapWrapperFix[A,B] {
      this: MapWrapper[A,B] =>
      override def containsKey(key: AnyRef): Boolean = try {
        get(key) != null
      } catch {
        case ex: ClassCastException => false
      }
    }

    implicit def mapAsJavaMap[A <: String, B](m: scala.collection.Map[A, B]): java.util.Map[A, B] = m match {
      case JMapWrapper(wrapped) => wrapped.asInstanceOf[java.util.Map[A, B]]
      case _ => new MapWrapper[A,B](m) with MapWrapperFix[A, B]
    }
  }
}
