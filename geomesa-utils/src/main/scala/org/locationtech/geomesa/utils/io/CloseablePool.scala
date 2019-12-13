/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.Closeable

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

/**
  * Pool for sharing a finite set of closeable resources
  *
  * @tparam T object type
  */
trait CloseablePool[T <: Closeable] extends Closeable {

  /**
    * Borrow an item from the pool. The item will be returned to the pool after execution.
    *
    * @param fn function to execute on the borrowed item
    * @tparam U return type
    * @return
    */
  def borrow[U](fn: T => U): U
}

object CloseablePool {

  /**
    * Create a pool
    *
    * @param factory method for instantiating pool objects
    * @param size max size of the pool
    * @tparam T object type
    * @return
    */
  def apply[T <: Closeable](factory: => T, size: Int = 8): CloseablePool[T] = {
    val config = new GenericObjectPoolConfig[T]()
    config.setMaxTotal(size)
    new CommonsPoolPool(factory, config)
  }

  /**
    * Apache commons-pool-backed pool
    *
    * @param create factory method for creating new pool objects
    * @param config pool configuration options
    * @tparam T object type
    */
  class CommonsPoolPool[T <: Closeable](create: => T, config: GenericObjectPoolConfig[T]) extends CloseablePool[T] {

    private val factory: BasePooledObjectFactory[T] = new BasePooledObjectFactory[T] {
      override def wrap(obj: T) = new DefaultPooledObject[T](obj)
      override def create(): T = CommonsPoolPool.this.create
      override def destroyObject(p: PooledObject[T]): Unit = p.getObject.close()
    }

    private val pool = new GenericObjectPool[T](factory, config)

    override def borrow[U](fn: T => U): U = {
      val t = pool.borrowObject()
      try { fn(t) } finally {
        pool.returnObject(t)
      }
    }

    override def close(): Unit = pool.close()
  }
}
