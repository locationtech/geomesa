/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro


import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.AbstractFeatureFactoryImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.utils.text.{ObjectPoolFactory, ObjectPoolUtils}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class AvroSimpleFeatureFactory extends AbstractFeatureFactoryImpl {

  override def createSimpleFeature(attrs: Array[AnyRef],
                                   sft: SimpleFeatureType,
                                   id: String): SimpleFeature = {
    val f = new AvroSimpleFeature(new FeatureIdImpl(id), sft)
    f.setAttributes(attrs)
    f
  }

  override def createSimpleFeautre(attrs: Array[AnyRef],
                                   descriptor: AttributeDescriptor,
                                   id: String): SimpleFeature =
    createSimpleFeature(attrs, descriptor.asInstanceOf[SimpleFeatureType], id)

}

object AvroSimpleFeatureFactory {
  def init = {
    Hints.putSystemDefault(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  }

  private val builderCache =
    Caffeine
      .newBuilder()
      .build(
        new CacheLoader[SimpleFeatureType, ObjectPoolUtils[SimpleFeatureBuilder]] {
          override def load(sft: SimpleFeatureType): ObjectPoolUtils[SimpleFeatureBuilder] =
            ObjectPoolFactory(new SimpleFeatureBuilder(sft, featureFactory), size = 1024)
        }
      )

  private val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  private val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

  def buildAvroFeature(sft: SimpleFeatureType, attrs: Seq[AnyRef], id: String) =
    builderCache.get(sft).withResource { builder =>
      builder.addAll(attrs)
      builder.buildFeature(id)
    }

  def featureBuilder(sft: SimpleFeatureType): SimpleFeatureBuilder = new SimpleFeatureBuilder(sft, featureFactory)
}
