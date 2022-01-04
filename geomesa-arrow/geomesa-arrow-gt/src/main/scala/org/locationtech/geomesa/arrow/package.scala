/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

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

    def getAllocatedMemory(typeName: String): Long =
      root.getChildAllocators.asScala
        .filter( a => a.getName.endsWith(s":$typeName"))
        .map(_.getAllocatedMemory)
        .sum

    /**
     * Forwards the getAllocatedMemory from the root Arrow Allocator
     * @return the number of bytes allocated off-heap by Arrow
     */
    def getAllocatedMemory: Long = root.getAllocatedMemory

    /**
     * Forwards the getPeakMemoryAllocation from the root Arrow Allocator
     * @return the peak number of bytes allocated off-heap by Arrow
     */
    def getPeakMemoryAllocation: Long = root.getPeakMemoryAllocation
  }

  object ArrowProperties {
    val BatchSize: SystemProperty = SystemProperty("geomesa.arrow.batch.size", "10000")
  }
}
