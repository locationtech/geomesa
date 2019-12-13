/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import java.util.concurrent.atomic.AtomicLong

import org.geotools.data.simple.SimpleFeatureWriter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object LambdaFeatureWriter {

  private val featureIds = new AtomicLong(0)

  class AppendLambdaFeatureWriter(transient: TransientStore) extends SimpleFeatureWriter {

    private var feature: SimpleFeature = _

    override def getFeatureType: SimpleFeatureType = transient.sft

    override def hasNext: Boolean = false

    override def next(): SimpleFeature = {
      feature = new ScalaSimpleFeature(transient.sft, featureIds.getAndIncrement().toString)
      feature
    }

    override def write(): Unit = {
      transient.write(feature)
      feature = null
    }

    override def remove(): Unit = throw new NotImplementedError()

    override def close(): Unit = {}
  }

  class ModifyLambdaFeatureWriter(transient: TransientStore, features: CloseableIterator[SimpleFeature])
      extends SimpleFeatureWriter {

    private var feature: SimpleFeature = _

    override def getFeatureType: SimpleFeatureType = transient.sft

    override def hasNext: Boolean = features.hasNext

    override def next(): SimpleFeature = {
      feature = ScalaSimpleFeature.copy(features.next())
      feature
    }

    override def write(): Unit = {
      transient.write(feature)
      feature = null
    }

    override def remove(): Unit = {
      transient.delete(feature)
      feature = null
    }

    override def close(): Unit = features.close()
  }
}
