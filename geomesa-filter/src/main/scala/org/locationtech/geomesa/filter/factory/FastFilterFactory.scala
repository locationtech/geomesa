/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.factory

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.expression._
import org.locationtech.geomesa.utils.geotools.{SimpleFeaturePropertyAccessor, SimpleFeatureTypes}
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.MultiValuedFilter.MatchAction
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.{Filter, FilterFactory2, PropertyIsEqualTo}
import org.xml.sax.helpers.NamespaceSupport

/**
  * Filter factory that creates optimized filters
  *
  * Note: usage expects the sft to be set in FastFilterFactory.sfts
  * FastFilterFactory.toFilter will handle this for you
 */
class FastFilterFactory private extends org.geotools.filter.FilterFactoryImpl with FilterFactory2 {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  override def property(name: String): PropertyName = {
    val sft = FastFilterFactory.sfts.get
    val colon = name.indexOf(":")
    val local = if (colon == -1) { name } else { name.substring(colon + 1) }
    val index = sft.indexOf(local)
    if (index != -1) {
      new FastPropertyName(name, index)
    } else {
      val sf = new SimpleFeatureBuilder(sft).buildFeature("")
      SimpleFeaturePropertyAccessor.getAccessor(sf, name) match {
        case Some(a) => new FastPropertyNameAccessor(name, a)
        case None => throw new RuntimeException(s"Can't handle property '$name' for feature type " +
            s"${sft.getTypeName} ${SimpleFeatureTypes.encodeType(sft)}")
      }
    }
  }

  override def property(name: Name): PropertyName = property(name.getLocalPart)

  override def property(name: String, namespaceContext: NamespaceSupport): PropertyName = property(name)

  override def equals(exp1: Expression, exp2: Expression): PropertyIsEqualTo =
    equal(exp1, exp2, matchCase = true, MatchAction.ANY)

  override def equal(exp1: Expression, exp2: Expression, matchCase: Boolean): PropertyIsEqualTo =
    equal(exp1, exp2, matchCase, MatchAction.ANY)

  override def equal(exp1: Expression, exp2: Expression, matchCase: Boolean, matchAction: MatchAction): PropertyIsEqualTo = {
    if (matchAction == MatchAction.ANY) {
      if (matchCase) {
        org.locationtech.geomesa.filter.checkOrder(exp1, exp2) match {
          case None => super.equal(exp1, exp2, matchCase, matchAction)
          case Some(prop) =>
            val descriptor = FastFilterFactory.sfts.get.getDescriptor(prop.name)
            if (descriptor == null || !descriptor.isList) {
              new FastIsEqualTo(exp1, exp2)
            } else if (prop.flipped) {
              // FastListIsEqual expects list to be in the first expression
              new FastListIsEqualToAny(exp2, exp1)
            } else {
              new FastListIsEqualToAny(exp1, exp2)
            }
        }
      } else {
        new FastIsEqualToIgnoreCase(exp1, exp2)
      }
    } else {
      super.equal(exp1, exp2, matchCase, matchAction)
    }
  }
}

object FastFilterFactory {

  val factory = new FastFilterFactory

  val sfts = new ThreadLocal[SimpleFeatureType]()

  def toFilter(sft: SimpleFeatureType, ecql: String): Filter = {
    sfts.set(sft)
    try {
      ECQL.toFilter(ecql, factory)
    } finally {
      sfts.remove()
    }
  }
}