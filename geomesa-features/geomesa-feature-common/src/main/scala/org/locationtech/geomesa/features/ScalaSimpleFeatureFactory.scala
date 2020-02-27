/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.AbstractFeatureFactoryImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.factory.Hints
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class ScalaSimpleFeatureFactory extends AbstractFeatureFactoryImpl {

  override def createSimpleFeature(attrs: Array[AnyRef], sft: SimpleFeatureType, id: String): ScalaSimpleFeature = {
    val sf = new ScalaSimpleFeature(sft, id)
    sf.setAttributes(attrs)
    sf
  }

  override def createSimpleFeautre(attrs: Array[AnyRef], descriptor: AttributeDescriptor, id: String): ScalaSimpleFeature =
    createSimpleFeature(attrs, descriptor.asInstanceOf[SimpleFeatureType], id)

}

object ScalaSimpleFeatureFactory {

  private val hints = new Hints(Hints.FEATURE_FACTORY, classOf[ScalaSimpleFeatureFactory])
  private val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

  def init(): Unit = Hints.putSystemDefault(Hints.FEATURE_FACTORY, classOf[ScalaSimpleFeatureFactory])

  def buildFeature(sft: SimpleFeatureType, attrs: Seq[AnyRef], id: String): SimpleFeature = {
    val builder = featureBuilder(sft)
    builder.addAll(attrs)
    builder.buildFeature(id)
  }

  def copyFeature(sft: SimpleFeatureType, feature: SimpleFeature, id: String): SimpleFeature = {
    val builder = featureBuilder(sft)
    builder.init(feature)
    builder.buildFeature(id)
  }

  def featureBuilder(sft: SimpleFeatureType): SimpleFeatureBuilder =
    new SimpleFeatureBuilder(sft, featureFactory)
}
