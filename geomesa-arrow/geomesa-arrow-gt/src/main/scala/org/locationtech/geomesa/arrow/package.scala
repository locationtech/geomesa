/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

package object arrow {

  // need to be lazy to avoid class loading issues before init is called
  lazy val ArrowEncodedSft: SimpleFeatureType =
    SimpleFeatureTypes.createType("arrow", "batch:Bytes,*geom:Point:srid=4326")

  object ArrowAllocator {

    private val root = new RootAllocator(Long.MaxValue)

    sys.addShutdownHook(CloseWithLogging(root))

    /**
     * Gets a new allocator from the root allocator. Allocator should be `close`d after use.
     *
     * The name does not matter, but will be included in log messages if the allocator is not
     * disposed of properly.
     *
     * @param name name of the allocator, for bookkeeping
     * @return
     */
    def apply(name: String): BufferAllocator = root.newChildAllocator(name, 0L, Long.MaxValue)
  }

  object ArrowProperties {
    val BatchSize: SystemProperty = SystemProperty("geomesa.arrow.batch.size", "10000")
  }

  case class TypeBindings(bindings: Seq[ObjectType], encoding: SimpleFeatureEncoding)
}
