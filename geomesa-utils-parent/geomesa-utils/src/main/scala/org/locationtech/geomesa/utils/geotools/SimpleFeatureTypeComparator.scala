/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.opengis.feature.simple.SimpleFeatureType

object SimpleFeatureTypeComparator {

  import TypeComparison._

  /**
   * Compares an existing feature type with an update to the feature type. This comparison
   * is specific to the types of updates that GeoMesa supports.
   *
   * @param existing existing type
   * @param update updated type
   * @return
   */
  def compare(existing: SimpleFeatureType, update: SimpleFeatureType): TypeComparison = {
    if (existing.getAttributeCount > update.getAttributeCount) {
      return AttributeRemoved
    }

    var renamed = false
    var superclass = false
    val attributeChangeBuilder = Map.newBuilder[String, (Class[_], Class[_])]

    // check for column type changes
    var i = 0
    while (i < existing.getAttributeCount) {
      val e = existing.getDescriptor(i)
      val u = update.getDescriptor(i)
      if (u.getType.getBinding != e.getType.getBinding) {
        if (u.getType.getBinding.isAssignableFrom(e.getType.getBinding)) {
          superclass = true
        } else {
          attributeChangeBuilder += e.getLocalName -> (e.getType.getBinding, u.getType.getBinding)
        }
      }
      renamed = renamed || u.getLocalName != e.getLocalName
      i += 1
    }

    val attributeChanges = attributeChangeBuilder.result()
    if (attributeChanges.nonEmpty) {
      AttributeTypeChanged(attributeChanges)
    } else {
      val extension = i < update.getAttributeCount
      Compatible(extension, renamed, superclass)
    }
  }

  sealed trait TypeComparison

  object TypeComparison {

    /**
     * Attributes have been removed from the schema
     */
    case object AttributeRemoved extends TypeComparison

    /**
     * Attribute types have changed in an incompatible manner
     */
    case class AttributeTypeChanged(changes: Map[String, (Class[_], Class[_])]) extends TypeComparison

    /**
     * Types are compatible, in that GeoMesa can support migrating from one to the other
     *
     * @param extension  attributes were added at the end
     * @param renamed    attributes were renamed but have the same bindings
     * @param superclass attributes are superclasses of the original binding
     */
    case class Compatible(extension: Boolean, renamed: Boolean, superclass: Boolean) extends TypeComparison
  }
}
