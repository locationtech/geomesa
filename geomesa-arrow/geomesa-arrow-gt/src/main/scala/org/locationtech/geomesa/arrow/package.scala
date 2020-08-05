/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.arrow.memory.{AllocationListener, AllocationOutcome, BufferAllocator, RootAllocator}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

package object arrow {

  val id = new AtomicInteger(0)
  // need to be lazy to avoid class loading issues before init is called
  lazy val ArrowEncodedSft: SimpleFeatureType =
    SimpleFeatureTypes.createType("arrow", "batch:Bytes,*geom:Point:srid=4326")

  object ArrowAllocator extends LazyLogging {

    val listener: AllocationListener = new AllocationListener with LazyLogging {
//      override def onPreAllocation(size: Long): Unit = {
//        println(s"Root is being called with onPreAllocation with argument $size")
//      }
//
//      override def onAllocation(size: Long): Unit = {
//        println(s"Root is being called with onAllocation with argument $size")
//      }
//      override def onRelease(size: Long): Unit = {
//        println(s"Root is being called with onRelease with argument $size")
//      }
//      override def onFailedAllocation(size: Long, outcome: AllocationOutcome): Boolean = {
//        println(s"onFailedAllocation has been called")
//        super.onFailedAllocation(size, outcome)
//      }

      override def onChildAdded(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
        logger.info(s"child allocator ${childAllocator.getName} has been added to ${parentAllocator.getName} in thread ${Thread.currentThread.getName}")
        val e = new Exception("Get Allocation Stack")
        logger.info(s"Creating allocator ${childAllocator.getName} in thread ${Thread.currentThread.getName} with stack ${e.getStackTrace.take(50).mkString("\n\t")}")

      }

      override def onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
        logger.info(s"child allocator ${childAllocator.getName} has been removed from ${parentAllocator.getName} in thread ${Thread.currentThread.getName} ")
        if (childAllocator.getName.startsWith("simple-feature-vector")) {
          val e = new Exception("Get Removal Stack")
          logger.info(s"Removing allocator ${childAllocator.getName} in thread ${Thread.currentThread.getName} with stack ${e.getStackTrace.take(50).mkString("\n\t")}")
        }
      }
    }

    private val root = new RootAllocator(listener, Long.MaxValue)

    sys.addShutdownHook( {
      logger.debug(s"Root arrow status: ${root.toVerboseString}")
      CloseWithLogging(root)
    })

    /**
     * Gets a new allocator from the root allocator. Allocator should be `close`d after use.
     *
     * The name does not matter, but will be included in log messages if the allocator is not
     * disposed of properly.
     *
     * @param name name of the allocator, for bookkeeping
     * @return
     */
    def apply(name: String): BufferAllocator = {
      root.newChildAllocator(s"$name-${id.getAndIncrement()}", 0L, Long.MaxValue)
    }

  }

  object ArrowProperties {
    val BatchSize: SystemProperty = SystemProperty("geomesa.arrow.batch.size", "10000")
  }

  case class TypeBindings(bindings: Seq[ObjectType], encoding: SimpleFeatureEncoding)
}
