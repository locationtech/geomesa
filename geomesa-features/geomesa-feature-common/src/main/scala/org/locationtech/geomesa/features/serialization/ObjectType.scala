/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import java.util.{UUID, Collections => jCollections, List => jList, Map => jMap}

import org.locationtech.jts.geom._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs.{USER_DATA_LIST_TYPE, USER_DATA_MAP_KEY_TYPE, USER_DATA_MAP_VALUE_TYPE}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.opengis.feature.`type`.AttributeDescriptor

object ObjectType extends Enumeration {

  type ObjectType = Value

  val STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, UUID, GEOMETRY, LIST, MAP, BYTES, JSON = Value

  val POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION = Value

  /**
    * @see selectType(clazz: Class[_], metadata: java.util.Map[_, _])
    *
    * @param descriptor attribute descriptor
    * @return
    */
  def selectType(descriptor: AttributeDescriptor): Seq[ObjectType] =
    selectType(descriptor.getType.getBinding, descriptor.getUserData)

  /**
    * Turns a SimpleFeatureType attribute class binding into an enumeration.
    *
    * The first element in the result will be the primary binding. For geometries, lists and maps,
    * the result will also contain secondary types.
    *
    * Lists will contain the type of the list elements.
    * Maps will contain the type of the map keys, then the type of the map values.
    * Geometries will contain the specific geometry type.
    *
    * Note: geometries will always return GEOMETRY as the primary type to allow for generic matching.
    *
    * @param clazz class, must be valid for a SimpleFeatureType attribute
    * @param metadata attribute metadata (user data)
    * @return binding
    */
  def selectType(clazz: Class[_], metadata: jMap[_, _] = jCollections.emptyMap()): Seq[ObjectType] = {
    clazz match {
      case c if classOf[java.lang.String].isAssignableFrom(c) =>
        if (metadata.get(OPT_JSON) == "true") { Seq(JSON) } else { Seq(STRING) }
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => Seq(INT)
      case c if classOf[java.lang.Long].isAssignableFrom(c) => Seq(LONG)
      case c if classOf[java.lang.Float].isAssignableFrom(c) => Seq(FLOAT)
      case c if classOf[java.lang.Double].isAssignableFrom(c) => Seq(DOUBLE)
      case c if classOf[java.lang.Boolean].isAssignableFrom(c) => Seq(BOOLEAN)
      case c if classOf[java.util.Date].isAssignableFrom(c) => Seq(DATE)
      case c if classOf[UUID].isAssignableFrom(c) => Seq(UUID)
      case c if classOf[Geometry].isAssignableFrom(c) => geometryType(c.asInstanceOf[Class[_ <: Geometry]])
      case c if classOf[Array[Byte]].isAssignableFrom(c) => Seq(BYTES)
      case c if classOf[jList[_]].isAssignableFrom(c) => listType(metadata)
      case c if classOf[jMap[_, _]].isAssignableFrom(c) => mapType(metadata)

      case _ => throw new IllegalArgumentException(s"Class $clazz can't be serialized")
    }
  }

  private def geometryType(clazz: Class[_ <: Geometry]): Seq[ObjectType] = {
    val subtype = clazz match {
      case c if c == classOf[Point]              => POINT
      case c if c == classOf[LineString]         => LINESTRING
      case c if c == classOf[Polygon]            => POLYGON
      case c if c == classOf[MultiLineString]    => MULTILINESTRING
      case c if c == classOf[MultiPolygon]       => MULTIPOLYGON
      case c if c == classOf[MultiPoint]         => MULTIPOINT
      case c if c == classOf[GeometryCollection] => GEOMETRY_COLLECTION
      case _                                     => GEOMETRY
    }
    Seq(GEOMETRY, subtype)
  }

  private def listType(metadata: jMap[_, _]): Seq[ObjectType] = {
    val clazz = Class.forName(metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[String])
    selectType(clazz) match {
      case Seq(binding) => Seq(LIST, binding)
      case _ => throw new IllegalArgumentException(s"Can't serialize list sub-type of ${clazz.getName}")
    }
  }

  private def mapType(metadata: jMap[_, _]): Seq[ObjectType] = {
    val keyClass   = Class.forName(metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[String])
    val keyType = selectType(keyClass) match {
      case Seq(binding) => binding
      case _ => throw new IllegalArgumentException(s"Can't serialize map key type of ${keyClass.getName}")
    }
    val valueClass = Class.forName(metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[String])
    val valueType = selectType(valueClass) match {
      case Seq(binding) => binding
      case _ => throw new IllegalArgumentException(s"Can't serialize map value type of ${valueClass.getName}")
    }
    Seq(MAP, keyType, valueType)
  }
}
