/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.{UUID, Collections => jCollections, List => jList, Map => jMap}

import org.geotools.feature.AttributeTypeBuilder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs.{UserDataListType, UserDataMapKeyType, UserDataMapValueType}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.locationtech.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor

object ObjectType extends Enumeration {

  type ObjectType = Value

  val STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, UUID, GEOMETRY, LIST, MAP, BYTES = Value

  // geometry sub-types
  val POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION = Value

  // string sub-types
  val JSON = Value

  private val geometryTypeMap = Map[Class[_], ObjectType](
    classOf[Point]              -> POINT,
    classOf[LineString]         -> LINESTRING,
    classOf[Polygon]            -> POLYGON,
    classOf[MultiLineString]    -> MULTILINESTRING,
    classOf[MultiPolygon]       -> MULTIPOLYGON,
    classOf[MultiPoint]         -> MULTIPOINT,
    classOf[GeometryCollection] -> GEOMETRY_COLLECTION,
    classOf[Geometry]           -> GEOMETRY
  )

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
        if (metadata.get(OptJson) == "true") { Seq(STRING, JSON) } else { Seq(STRING) }
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => Seq(INT)
      case c if classOf[java.lang.Long].isAssignableFrom(c) => Seq(LONG)
      case c if classOf[java.lang.Float].isAssignableFrom(c) => Seq(FLOAT)
      case c if classOf[java.lang.Double].isAssignableFrom(c) => Seq(DOUBLE)
      case c if classOf[java.lang.Boolean].isAssignableFrom(c) => Seq(BOOLEAN)
      case c if classOf[java.util.Date].isAssignableFrom(c) => Seq(DATE)
      case c if classOf[UUID].isAssignableFrom(c) => Seq(UUID)
      case c if classOf[Geometry].isAssignableFrom(c) => Seq(GEOMETRY, geometryTypeMap(c))
      case c if classOf[Array[Byte]].isAssignableFrom(c) => Seq(BYTES)
      case c if classOf[jList[_]].isAssignableFrom(c) => listType(metadata)
      case c if classOf[jMap[_, _]].isAssignableFrom(c) => mapType(metadata)

      case _ => throw new IllegalArgumentException(s"Class $clazz can't be serialized")
    }
  }

  /**
   * Create an attribute descriptor from a type binding
   *
   * @param name attribute name
   * @param bindings bindings
   * @return
   */
  def createDescriptor(name: String, bindings: Seq[ObjectType]): AttributeDescriptor = {
    val builder = new AttributeTypeBuilder()

    selectBindings(bindings) match {
      case Seq(c) =>
        builder.binding(c)

      case Seq(c1, c2) if c1 == classOf[java.util.List[_]] =>
        builder.binding(c1)
        builder.userData(AttributeConfigs.UserDataListType, c2.getName)

      case Seq(c1, c2, c3) if c1 == classOf[java.util.Map[_, _]] =>
        builder.binding(c1)
        builder.userData(AttributeConfigs.UserDataMapKeyType, c2.getName)
        builder.userData(AttributeConfigs.UserDataMapValueType, c3.getName)

      case b =>
        throw new IllegalArgumentException(s"Unexpeced bindings: ${bindings}: $b")
    }

    builder.buildDescriptor(name)
  }

  private def listType(metadata: jMap[_, _]): Seq[ObjectType] = {
    val clazz = Class.forName(metadata.get(UserDataListType).asInstanceOf[String])
    selectType(clazz) match {
      case Seq(binding) => Seq(LIST, binding)
      case _ => throw new IllegalArgumentException(s"Can't serialize list sub-type of ${clazz.getName}")
    }
  }

  private def mapType(metadata: jMap[_, _]): Seq[ObjectType] = {
    val keyClass = Class.forName(metadata.get(UserDataMapKeyType).asInstanceOf[String])
    val keyType = selectType(keyClass) match {
      case Seq(binding) => binding
      case _ => throw new IllegalArgumentException(s"Can't serialize map key type of ${keyClass.getName}")
    }
    val valueClass = Class.forName(metadata.get(UserDataMapValueType).asInstanceOf[String])
    val valueType = selectType(valueClass) match {
      case Seq(binding) => binding
      case _ => throw new IllegalArgumentException(s"Can't serialize map value type of ${valueClass.getName}")
    }
    Seq(MAP, keyType, valueType)
  }

  private def selectBindings(types: Seq[ObjectType]): Seq[Class[_]] = {
    types.head match {
      case STRING   => Seq(classOf[java.lang.String])
      case INT      => Seq(classOf[java.lang.Integer])
      case LONG     => Seq(classOf[java.lang.Long])
      case FLOAT    => Seq(classOf[java.lang.Float])
      case DOUBLE   => Seq(classOf[java.lang.Double])
      case BOOLEAN  => Seq(classOf[java.lang.Boolean])
      case DATE     => Seq(classOf[java.util.Date])
      case UUID     => Seq(classOf[UUID])
      case BYTES    => Seq(classOf[Array[Byte]])
      case GEOMETRY => geometryTypeMap.collectFirst { case (k, v) if v == types.last => k }.toSeq
      case LIST     => Seq(classOf[java.util.List[_]], selectBindings(types.tail).head)
      case MAP      => Seq(classOf[java.util.Map[_, _]], selectBindings(types.slice(1, 2)).head, selectBindings(types.slice(2, 3)).head)

      case _ => throw new IllegalArgumentException(s"Unexpected type: ${types.head}")
    }
  }
}
