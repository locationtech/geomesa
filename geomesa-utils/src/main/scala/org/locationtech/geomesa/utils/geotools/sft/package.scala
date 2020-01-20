/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Collections

import org.geotools.feature.`type`._
import org.geotools.feature.simple.SimpleFeatureTypeImpl
import org.opengis.feature.`type`._
import org.opengis.filter.Filter
import org.opengis.util.InternationalString

package object sft {

  // claim: the default simple feature type implementations are immutable except for user data.
  // these classes override the user data to make it immutable

  //  note that some parts of the feature type may still be mutable - in particular AttributeType,
  //  GeometryType and SuperType are not used by geomesa so we don't bother with them. In addition,
  //  user data keys and values may be mutable objects, so while the user data map will not change,
  //  the values inside may

  class ImmutableAttributeDescriptor(
      `type`: AttributeType,
      name: Name,
      minOccurs: Int,
      maxOccurs: Int,
      isNillable: Boolean,
      defaultValue: AnyRef,
      ud: java.util.Map[_ <: AnyRef, _ <: AnyRef]
    ) extends AttributeDescriptorImpl(`type`, name, minOccurs, maxOccurs, isNillable, defaultValue) {
    override val getUserData: java.util.Map[AnyRef, AnyRef] =
      Collections.unmodifiableMap(new java.util.HashMap[AnyRef, AnyRef](ud))
  }

  class ImmutableGeometryDescriptor(
      `type`: GeometryType,
      name: Name,
      minOccurs: Int,
      maxOccurs: Int,
      isNillable: Boolean,
      defaultValue: AnyRef,
      ud: java.util.Map[_ <: AnyRef, _ <: AnyRef]
    ) extends GeometryDescriptorImpl(`type`, name, minOccurs, maxOccurs, isNillable, defaultValue) {
    override val getUserData: java.util.Map[AnyRef, AnyRef] =
      Collections.unmodifiableMap(new java.util.HashMap[AnyRef, AnyRef](ud))
  }

  class ImmutableSimpleFeatureType(
      name: Name,
      schema: java.util.List[AttributeDescriptor],
      defaultGeometry: GeometryDescriptor,
      isAbstract: Boolean,
      restrictions: java.util.List[Filter],
      superType: AttributeType,
      description: InternationalString,
      ud: java.util.Map[_ <: AnyRef, _ <: AnyRef]
    ) extends SimpleFeatureTypeImpl(name, schema, defaultGeometry, isAbstract, restrictions, superType, description) {
    override val getUserData: java.util.Map[AnyRef, AnyRef] =
      Collections.unmodifiableMap(new java.util.HashMap[AnyRef, AnyRef](ud))
    override lazy val toString: String = s"SimpleFeatureType $name ${SimpleFeatureTypes.encodeType(this)}"
  }
}
