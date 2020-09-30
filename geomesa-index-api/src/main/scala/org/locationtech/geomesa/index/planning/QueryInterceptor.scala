/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.io.Closeable
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.filter.Bounds
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.index.TemporalIndexValues
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try
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

      private val loader =  new CacheLoader[String, Seq[QueryInterceptor]]() {
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

      private val cache = Caffeine.newBuilder().refreshAfterWrite(expiry, TimeUnit.MILLISECONDS).build(loader)

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

  class TemporalQueryGuard extends QueryInterceptor {

    import TemporalQueryGuard.Config
    import org.locationtech.geomesa.filter.filterToString

    private var max: Duration = _

    override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
      max = Try(Duration(sft.getUserData.get(Config).asInstanceOf[String])).getOrElse {
        throw new IllegalArgumentException(
          s"Temporal query guard expects valid duration under user data key '$Config'")
      }
    }

    override def rewrite(query: Query): Unit = {}

    override def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = {
      val intervals = strategy.values.collect { case v: TemporalIndexValues => v.intervals }
      intervals.collect { case i if i.isEmpty || !i.forall(_.isBoundedBothSides) || duration(i.values) > max =>
          new IllegalArgumentException(
            s"Query exceeds maximum allowed filter duration of $max: ${filterToString(strategy.filter.filter)}")
      }
    }

    override def close(): Unit = {}

    private def duration(values: Seq[Bounds[ZonedDateTime]]): FiniteDuration = {
      values.foldLeft(Duration.Zero) { (sum, bounds) =>
        sum + Duration(bounds.upper.value.get.toEpochSecond - bounds.lower.value.get.toEpochSecond, TimeUnit.SECONDS)
      }
    }
  }

  object TemporalQueryGuard {
    val Config = "geomesa.filter.max.duration"
  }
}
