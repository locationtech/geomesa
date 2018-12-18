/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Provides a hook to modify a query before executing it
  */
trait QueryInterceptor extends Closeable {

  /**
    * Called exactly once after the interceptor is instantiated
    *
    * @param ds data store
    * @param sft simple feature type
    */
  def init(ds: DataStore, sft: SimpleFeatureType): Unit

  /**
    * Modifies the query in place
    *
    * @param query query
    */
  def rewrite(query: Query): Unit
}

object QueryInterceptor extends LazyLogging {

  /**
    * Manages query interceptors
    */
  trait QueryInterceptorFactory extends Closeable {
    def apply(sft: SimpleFeatureType): Seq[QueryInterceptor]
  }

  object QueryInterceptorFactory {

    private val Empty: QueryInterceptorFactory = new QueryInterceptorFactory {
      override def apply(sft: SimpleFeatureType): Seq[QueryInterceptor] = Seq.empty
      override def close(): Unit = {}
    }

    def apply(ds: DataStore): QueryInterceptorFactory = new QueryInterceptorFactoryImpl(ds)

    def empty(): QueryInterceptorFactory = Empty

    /**
      * Manages query interceptor lifecycles for a given data store
      *
      * @param ds data store
      */
    private class QueryInterceptorFactoryImpl(ds: DataStore) extends QueryInterceptorFactory {

      import scala.collection.JavaConverters._

      private val expiry = CachedLazyMetadata.Expiry.toDuration.get.toMillis

      private val loader =  new CacheLoader[String, Seq[QueryInterceptor]]() {
        override def load(key: String): Seq[QueryInterceptor] = QueryInterceptorFactoryImpl.this.load(key)
        override def reload(key: String, oldValue: Seq[QueryInterceptor]): Seq[QueryInterceptor] = {
          // only recreate the interceptors if they have changed
          Option(ds.getSchema(key)).flatMap(s => Option(s.getUserData.get(Configs.QUERY_INTERCEPTORS))) match {
            case None if oldValue.isEmpty => oldValue
            case Some(classes) if classes == oldValue.map(_.getClass.getName).mkString(",") => oldValue
            case _ => oldValue.foreach(CloseWithLogging.apply); QueryInterceptorFactoryImpl.this.load(key)
          }
        }
      }

      private val cache = Caffeine.newBuilder().refreshAfterWrite(expiry, TimeUnit.MILLISECONDS).build(loader)

      override def apply(sft: SimpleFeatureType): Seq[QueryInterceptor] = cache.get(sft.getTypeName)

      override def close(): Unit = {
        cache.asMap.asScala.foreach { case (_, interceptors) => interceptors.foreach(CloseWithLogging.apply) }
        cache.invalidateAll()
      }

      private def load(typeName: String): Seq[QueryInterceptor] = {
        val sft = ds.getSchema(typeName)
        if (sft == null) { Seq.empty } else {
          val classes = sft.getUserData.get(Configs.QUERY_INTERCEPTORS).asInstanceOf[String]
          if (classes == null || classes.isEmpty) { Seq.empty } else {
            classes.split(",").toSeq.flatMap { c =>
              var interceptor: QueryInterceptor = null
              try {
                interceptor = Class.forName(c).newInstance().asInstanceOf[QueryInterceptor]
                interceptor.init(ds, sft)
                Seq(interceptor)
              } catch {
                case NonFatal(e) =>
                  logger.error(s"Error creating query interceptor '$c':", e)
                  if (interceptor != null) {
                    CloseWithLogging(interceptor)
                  }
                  Seq.empty
              }
            }
          }
        }
      }
    }
  }
}
