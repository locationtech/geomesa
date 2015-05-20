/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.serialization

import java.util.{Collections => jCollections, List => jList, Map => jMap, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._

object ObjectType extends Enumeration {

  type ObjectType = Value

  val STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, UUID, GEOMETRY, HINTS, LIST, MAP = Value

  def selectType(clazz: Class[_], metadata: jMap[_, _] = jCollections.emptyMap()): (ObjectType, Seq[ObjectType]) = {
    clazz match {
      case c if classOf[java.lang.String].isAssignableFrom(c) => (STRING, Seq.empty)
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => (INT, Seq.empty)
      case c if classOf[java.lang.Long].isAssignableFrom(c) => (LONG, Seq.empty)
      case c if classOf[java.lang.Float].isAssignableFrom(c) => (FLOAT, Seq.empty)
      case c if classOf[java.lang.Double].isAssignableFrom(c) => (DOUBLE, Seq.empty)
      case c if classOf[java.lang.Boolean].isAssignableFrom(c) => (BOOLEAN, Seq.empty)
      case c if classOf[java.util.Date].isAssignableFrom(c) => (DATE, Seq.empty)
      case c if classOf[UUID].isAssignableFrom(c) => (UUID, Seq.empty)
      case c if classOf[Geometry].isAssignableFrom(c) => (GEOMETRY, Seq.empty)
      case c if classOf[Hints.Key].isAssignableFrom(c) => (HINTS, Seq.empty)
      case c if classOf[jList[_]].isAssignableFrom(c) =>
        (LIST, Seq(metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[Class[_]]).map(selectType(_)._1))
      case c if classOf[jMap[_, _]].isAssignableFrom(c) =>
        val keyClass   = metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[Class[_]]
        val valueClass = metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[Class[_]]
        (MAP, Seq(keyClass, valueClass).map(selectType(_)._1))
      case _ => throw new IllegalArgumentException(s"Class $clazz can't be serialized")
    }
  }
}
