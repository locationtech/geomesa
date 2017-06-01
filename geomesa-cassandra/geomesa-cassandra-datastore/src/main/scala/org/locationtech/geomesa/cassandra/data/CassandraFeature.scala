/***********************************************************************
 * Copyright (c) 2017 IBM
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.cassandra.index.CassandraFeatureIndex
import org.locationtech.geomesa.index.api.WrappedFeature
import org.opengis.feature.simple.SimpleFeature

class CassandraFeature(val feature: SimpleFeature, serializer: SimpleFeatureSerializer) extends WrappedFeature {
  lazy val fullValue = serializer.serialize(feature)
}
