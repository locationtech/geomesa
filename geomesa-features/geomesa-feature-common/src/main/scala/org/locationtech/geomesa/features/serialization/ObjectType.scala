/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import java.util.{UUID, Collections => jCollections, List => jList, Map => jMap}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._

object ObjectType extends Enumeration {

  type ObjectType = Value

  val STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, UUID, GEOMETRY, HINTS, LIST, MAP, BYTES, JSON = Value

  def selectType(clazz: Class[_], metadata: jMap[_, _] = jCollections.emptyMap()): (ObjectType, Seq[ObjectType]) = {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs._

    clazz match {
      case c if classOf[java.lang.String].isAssignableFrom(c) =>
        if (metadata.get(OPT_JSON) == "true") { (JSON, Seq.empty) } else { (STRING, Seq.empty) }
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
        (LIST, Seq(Class.forName(metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[String])).map(selectType(_)._1))
      case c if classOf[jMap[_, _]].isAssignableFrom(c) =>
        val keyClass   = Class.forName(metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[String])
        val valueClass = Class.forName(metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[String])
        (MAP, Seq(keyClass, valueClass).map(selectType(_)._1))
      case c if classOf[Array[Byte]].isAssignableFrom(c) => (BYTES, Seq.empty)
      case _ => throw new IllegalArgumentException(s"Class $clazz can't be serialized")
    }
  }
}
