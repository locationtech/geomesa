/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import java.util.concurrent.atomic.AtomicInteger

import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable

package object arrow {
  val id = new AtomicInteger(0)

  // need to be lazy to avoid class loading issues before init is called
  lazy val ArrowEncodedSft: SimpleFeatureType =
    SimpleFeatureTypes.createType("arrow", "batch:Bytes,*geom:Point:srid=4326")

  object DelegatingAllocationListener extends AllocationListener {
    val listeners: mutable.Set[AllocationListener] = mutable.Set[AllocationListener]()

    def addListener(listener: AllocationListener): Boolean = listeners.add(listener)

    def removeListener(listener: AllocationListener): Boolean = listeners.remove(listener)

    override def onChildAdded(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit =
      listeners.foreach { _.onChildAdded(parentAllocator, childAllocator) }

    override def onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit =
      listeners.foreach { _.onChildRemoved(parentAllocator, childAllocator) }
  }

  class MatchingAllocationListener extends AllocationListener {
    val unmatchedAllocation: mutable.Set[String] = mutable.Set[String]()
    override def onChildAdded(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
      println(s"child allocator ${childAllocator.getName} added In Thread: ${Thread.currentThread().getName}")
      unmatchedAllocation.add(childAllocator.getName)
    }

    override def onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
      println(s"child allocator ${childAllocator.getName} removed In Thread: ${Thread.currentThread().getName}")
      unmatchedAllocation.remove(childAllocator.getName)
    }
  }


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
//        val e = new Exception("Get Allocation Stack")
//        logger.info(s"Creating allocator ${childAllocator.getName} in thread ${Thread.currentThread.getName} with stack ${e.getStackTrace.take(50).mkString("\n\t")}")

      }

      override def onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
        logger.info(s"child allocator ${childAllocator.getName} has been removed from ${parentAllocator.getName} in thread ${Thread.currentThread.getName} ")
//        if (childAllocator.getName.startsWith("simple-feature-vector")) {
//          val e = new Exception("Get Removal Stack")
//          logger.info(s"Removing allocator ${childAllocator.getName} in thread ${Thread.currentThread.getName} with stack ${e.getStackTrace.take(50).mkString("\n\t")}")
//        }
      }
    }

    private val root = new RootAllocator(DelegatingAllocationListener, Long.MaxValue)  // JNH: With lots of logging.
    //private val root = new RootAllocator(Long.MaxValue)

    sys.addShutdownHook({
      logger.error(s"At shutdown root arrow status: ${root.toVerboseString}")
      //println(s"Root arrow status: ${root.toVerboseString}")
      CloseWithLogging(root)
    })


    private val es = Executors.newSingleThreadScheduledExecutor()
    es.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        logger.error(s"Direct Memory status: MAX_DIRECT_MEMORY: ${PlatformDependent.maxDirectMemory()} DIRECT_MEMORY_COUNTER: ${getNettyMemoryCounter}")

        logger.error(s"Root arrow status: ${root.toVerboseString}")

      }
    }, 0, 1, TimeUnit.MINUTES)

//>>>>>>> e4049c4204... Previous fixes for Arrow memory leaks.  Still need to review.

    def getNettyMemoryCounter: Long = {
      try {
        val clazz = try {
          Class.forName("io.netty.util.internal.PlatformDependent")
        } catch {
          case _: Throwable =>
            Class.forName("org.locationtech.geomesa.accumulo.shade.io.netty.util.internal.PlatformDependent")
        }
        val field = clazz.getDeclaredField("DIRECT_MEMORY_COUNTER")
        field.setAccessible(true)
        field.get(clazz).asInstanceOf[java.util.concurrent.atomic.AtomicLong].get()
      } catch {
        case t: Throwable =>
          logger.error("failed to get DIRECT_MEMORY_COUNTER", t)
          -1
      }
    }

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

  case class TypeBindings(bindings: Seq[ObjectType], encoding: SimpleFeatureEncoding)
}
