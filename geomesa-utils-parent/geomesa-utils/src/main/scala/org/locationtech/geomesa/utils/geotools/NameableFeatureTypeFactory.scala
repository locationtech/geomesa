/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.api.feature.`type`.{AttributeDescriptor, AttributeType, GeometryDescriptor, Name}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.api.util.InternationalString
import org.geotools.feature.NameImpl
import org.geotools.feature.`type`.FeatureTypeFactoryImpl
import org.geotools.feature.simple.SimpleFeatureTypeImpl
import org.locationtech.geomesa.utils.geotools.NameableFeatureTypeFactory.{MutableName, NameableSimpleFeatureType}

/**
  * Feature type factory that allows for changing type name
  */
class NameableFeatureTypeFactory extends FeatureTypeFactoryImpl {

  override def createSimpleFeatureType(name: Name,
                                       schema: java.util.List[AttributeDescriptor] ,
                                       defaultGeometry: GeometryDescriptor,
                                       isAbstract: Boolean,
                                       restrictions: java.util.List[Filter],
                                       superType: AttributeType,
                                       description: InternationalString): SimpleFeatureType = {
    val n = new MutableName(name)
    new NameableSimpleFeatureType(n, schema, defaultGeometry, isAbstract, restrictions, superType, description)
  }
}

object NameableFeatureTypeFactory {

  class NameableSimpleFeatureType(name: MutableName,
                                  schema: java.util.List[AttributeDescriptor],
                                  defaultGeometry: GeometryDescriptor,
                                  isAbstract: Boolean,
                                  restrictions: java.util.List[Filter],
                                  superType: AttributeType,
                                  description: InternationalString)
      extends SimpleFeatureTypeImpl(name, schema, defaultGeometry, isAbstract, restrictions, superType, description) {

    def setName(namespace: String, local: String): Unit = name.setName(namespace, local)
  }

  class MutableName(name: Name) extends NameImpl(name.getNamespaceURI, name.getSeparator, name.getLocalPart) {
    def setName(namespace: String, local: String): Unit = {
      this.namespace = namespace
      this.local = local
    }
  }
}
