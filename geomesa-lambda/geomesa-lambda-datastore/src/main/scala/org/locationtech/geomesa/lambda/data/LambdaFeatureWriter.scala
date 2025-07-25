/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.{FastSettableFeature, ScalaSimpleFeature}
import org.locationtech.geomesa.index.geotools.FastSettableFeatureWriter
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.security.VisibilityChecker
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.util.concurrent.atomic.AtomicLong

object LambdaFeatureWriter {

  private val featureIds = new AtomicLong(0)

  class AppendLambdaFeatureWriter(transient: TransientStore) extends FastSettableFeatureWriter {

    protected var feature: ScalaSimpleFeature = _

    override def getFeatureType: SimpleFeatureType = transient.sft

    override def hasNext: Boolean = false

    override def next(): FastSettableFeature = {
      feature = new ScalaSimpleFeature(transient.sft, featureIds.getAndIncrement().toString)
      feature
    }

    override def write(): Unit = {
      transient.write(feature)
      feature = null
    }

    override def remove(): Unit = throw new UnsupportedOperationException()

    override def close(): Unit = {}
  }

  class ModifyLambdaFeatureWriter(transient: TransientStore, features: CloseableIterator[SimpleFeature])
      extends AppendLambdaFeatureWriter(transient) {

    override def hasNext: Boolean = features.hasNext

    override def next(): FastSettableFeature = {
      feature = ScalaSimpleFeature.copy(features.next())
      feature
    }

    override def remove(): Unit = {
      transient.delete(feature)
      feature = null
    }

    override def close(): Unit = features.close()
  }

  trait RequiredVisibilityWriter extends AppendLambdaFeatureWriter with VisibilityChecker {
    abstract override def write(): Unit = {
      requireVisibilities(feature)
      super.write()
    }
  }
}
