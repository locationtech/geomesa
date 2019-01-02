/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

package object arrow {

  implicit val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  // need to be lazy to avoid class loading issues before init is called
  lazy val ArrowEncodedSft: SimpleFeatureType =
    SimpleFeatureTypes.createType("arrow", "batch:Bytes,*geom:Point:srid=4326")

  object ArrowProperties {
    val BatchSize = SystemProperty("geomesa.arrow.batch.size", "10000")
  }

  case class TypeBindings(bindings: Seq[ObjectType], encoding: SimpleFeatureEncoding)
}
