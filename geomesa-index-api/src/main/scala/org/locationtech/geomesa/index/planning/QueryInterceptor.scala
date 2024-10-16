/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStore, Query}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.util.concurrent.TimeUnit
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

  /**
   * Hook to allow interception of a query after extracting the query values
   *
   * @param strategy query strategy
   * @return an exception if the query should be stopped
   */
  def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = None
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

      private val expiry = TableBasedMetadata.Expiry.toDuration.get.toMillis

      private val loader = new CacheLoader[String, Seq[QueryInterceptor]]() {
        override def load(key: String): Seq[QueryInterceptor] = QueryInterceptorFactoryImpl.this.load(key)
        override def reload(key: String, oldValue: Seq[QueryInterceptor]): Seq[QueryInterceptor] = {
          // only recreate the interceptors if they have changed
          Option(ds.getSchema(key)).flatMap(s => Option(s.getUserData.get(Configs.QueryInterceptors))) match {
            case None if oldValue.isEmpty => oldValue
            case Some(classes) if classes == oldValue.map(_.getClass.getName).mkString(",") => oldValue
            case _ => CloseWithLogging(oldValue); QueryInterceptorFactoryImpl.this.load(key)
          }
        }
      }

      private val cache: LoadingCache[String, Seq[QueryInterceptor]] =
        Caffeine.newBuilder().refreshAfterWrite(expiry, TimeUnit.MILLISECONDS).build(loader)

      override def apply(sft: SimpleFeatureType): Seq[QueryInterceptor] = cache.get(sft.getTypeName)

      override def close(): Unit = {
        cache.asMap.asScala.foreach { case (_, interceptors) => CloseWithLogging(interceptors) }
        cache.invalidateAll()
      }

      private def load(typeName: String): Seq[QueryInterceptor] = {
        val sft = ds.getSchema(typeName)
        if (sft == null) { Seq.empty } else {
          val classes = sft.getUserData.get(Configs.QueryInterceptors).asInstanceOf[String]
          if (classes == null || classes.isEmpty) { Seq.empty } else {
            classes.split(",").toSeq.flatMap { c =>
              var interceptor: QueryInterceptor = null
              try {
                interceptor = Class.forName(c).getDeclaredConstructor().newInstance().asInstanceOf[QueryInterceptor]
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
