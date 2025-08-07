/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs.{UserDataListType, UserDataMapKeyType, UserDataMapValueType}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.locationtech.jts.geom._

import java.util.{Collections, UUID, List => jList, Map => jMap}

object ObjectType extends Enumeration {

  type ObjectType = Value

  val STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, UUID, GEOMETRY, LIST, MAP, BYTES = Value

  // geometry sub-types
  val POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION = Value

  // string sub-types
  val JSON: Value = Value

  val GeometrySubtypes: Seq[Value] =
    Seq(POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION, GEOMETRY)

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
   * @param descriptor attribute descriptor
   * @return binding
   */
  def selectType(descriptor: AttributeDescriptor): Seq[ObjectType] =
    selectType(descriptor.getType.getBinding, descriptor.getUserData, Option(descriptor.getLocalName))

  /**
   * Used if there is no descriptor available. Note that this will throw an error for list or map-type attributes.
   *
   * @see selectType(descriptor: AttributeDescriptor)
   *
   * @param clazz class, must be valid for a SimpleFeatureType attribute
   * @return binding
   */
  def selectType(clazz: Class[_]): Seq[ObjectType] = selectType(clazz, Collections.emptyMap(), None)

  /**
   * Get the type enumeration
   *
   * @param clazz class, must be valid for a SimpleFeatureType attribute
   * @param metadata attribute metadata (user data)
   * @param fieldName name of the field
   * @return binding
   */
  private def selectType(clazz: Class[_], metadata: jMap[_, _], fieldName: Option[String]): Seq[ObjectType] = {
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
      case c if classOf[Geometry].isAssignableFrom(c) => geometryType(c.asInstanceOf[Class[_ <: Geometry]])
      case c if classOf[Array[Byte]].isAssignableFrom(c) => Seq(BYTES)
      case c if classOf[jList[_]].isAssignableFrom(c) => listType(metadata, fieldName)
      case c if classOf[jMap[_, _]].isAssignableFrom(c) => mapType(metadata, fieldName)

      case _ =>
        throw new IllegalArgumentException(s"${fieldName.fold("Type")(f => s"Field $f of type")} $clazz can't be serialized")
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

  private def listType(metadata: jMap[_, _], fieldName: Option[String]): Seq[ObjectType] = {
    val elementType = getSubType(metadata, UserDataListType, fieldName)
    Seq(LIST, elementType)
  }

  private def mapType(metadata: jMap[_, _], fieldName: Option[String]): Seq[ObjectType] = {
    val keyType = getSubType(metadata, UserDataMapKeyType, fieldName)
    val valueType = getSubType(metadata, UserDataMapValueType, fieldName)
    Seq(MAP, keyType, valueType)
  }

  private def getSubType(metadata: jMap[_, _], typeKey: String, fieldName: Option[String]): ObjectType = {
    def fieldError: String = s"for collection-type field${fieldName.fold("")(f => s" '$f'")}"
    metadata.get(typeKey) match {
      case s: String =>
        val clazz = Class.forName(s)
        selectType(clazz) match {
          case Seq(binding) => binding
          case _ => throw new IllegalArgumentException(s"Can't serialize sub-type of ${clazz.getName} $fieldError")
        }

      case null => throw new IllegalArgumentException(s"Missing user data key '$typeKey' $fieldError")
      case t => throw new IllegalArgumentException(s"Unexpected user data '$typeKey' $fieldError: $t")
    }
  }
}
