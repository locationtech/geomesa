/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.util.Objects

import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Plan for querying a GeoMesaDataStore
  *
  * @tparam DS type of this data store
  */
trait QueryPlan[DS <: GeoMesaDataStore[DS]] {

  /**
    * Type of raw results returned from the underlying database
    */
  type Results

  /**
    * Reference back to the strategy
    *
    * @return
    */
  def filter: FilterStrategy

  /**
    * Runs the query plan against the underlying database
    *
    * @param ds data store - provides connection object and metadata
    * @return
    */
  def scan(ds: DS): CloseableIterator[Results]

  /**
    * Transform results coming back from a raw scan into features
    *
    * @return
    */
  def resultsToFeatures: ResultsToFeatures[Results]

  /**
    * Optional reduce step for simple features coming back
    *
    * @return
    */
  def reducer: Option[FeatureReducer]

  /**
    * Sort fields and reverse order flags (true flips order to descending, false keeps order as ascending)
    *
    * @return
    */
  def sort: Option[Seq[(String, Boolean)]]

  /**
    * Max features to return from the scan
    *
    * @return
    */
  def maxFeatures: Option[Int]

  /**
    * Geometry projection
    *
    * @return
    */
  def projection: Option[QueryReferenceSystems]

  /**
    * Explains details on how this query plan will be executed
    *
    * @param explainer explainer to use for explanation
    * @param prefix prefix for explanation lines, used for nesting explanations
    */
  def explain(explainer: Explainer, prefix: String = ""): Unit
}

object QueryPlan {

  /**
    * Convert scan results to simple features. Must have a zero-arg constructor to allow re-creation from
    * a serialized form.
    *
    * Converters are encouraged to also allow direct instantiation via an alternate constructor, as
    * serialization is generally only used for map/reduce jobs. Similarly, state is encouraged to be
    * lazily evaluated.
    *
    * @tparam T result type
    */
  trait ResultsToFeatures[T] extends SerializableState {

    /**
      * Simple feature type that will be returned from `apply`
      *
      * @return
      */
    def schema: SimpleFeatureType

    /**
      * Convert a result to a feature
      *
      * @param result result
      * @return
      */
    def apply(result: T): SimpleFeature
  }

  object ResultsToFeatures {

    /**
      * Serialize a results to features as a string
      *
      * @param obj object
      * @tparam T result type
      * @return
      */
    def serialize[T](obj: ResultsToFeatures[T]): String = SerializableState.serialize(obj)

    /**
      * Deserialize a results to features from a string
      *
      * @param serialized serialized object
      * @tparam T result type
      * @return
      */
    def deserialize[T](serialized: String): ResultsToFeatures[T] = SerializableState.deserialize(serialized)

    /**
      * Empty results to features used in placeholders - don't invoke `apply` on the result.
      *
      * @tparam T result type
      * @return
      */
    def empty[T]: ResultsToFeatures[T] = EmptyResultsToFeatures.asInstanceOf[ResultsToFeatures[T]]

    /**
      * Identity function
      *
      * @return
      */
    def identity(sft: SimpleFeatureType): ResultsToFeatures[SimpleFeature] = new IdentityResultsToFeatures(sft)

    /**
      * For 'empty' query plans - don't invoke `apply`
      */
    object EmptyResultsToFeatures extends ResultsToFeatures[Void] {
      override val state: Map[String, String] = Map.empty
      override def init(state: Map[String, String]): Unit = {}
      override def schema: SimpleFeatureType = null
      override def apply(result: Void): SimpleFeature = null
    }

    /**
      * Identity function - for situations where features are already deserialized
      *
      * @param sft simple feature type
      */
    class IdentityResultsToFeatures(private var sft: SimpleFeatureType) extends ResultsToFeatures[SimpleFeature] {

      def this() = this(null) // no-arg constructor required for serialization

      override def state: Map[String, String] = Map(
        "name" -> sft.getTypeName,
        "spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true)
      )

      override def init(state: Map[String, String]): Unit =
        sft = SimpleFeatureTypes.createType(state("name"), state("spec"))

      override def schema: SimpleFeatureType = sft

      override def apply(result: SimpleFeature): SimpleFeature = result

      def canEqual(other: Any): Boolean = other.isInstanceOf[IdentityResultsToFeatures]

      override def equals(other: Any): Boolean = other match {
        case that: IdentityResultsToFeatures if that.canEqual(this) => sft == that.sft
        case _ => false
      }

      override def hashCode(): Int = {
        val state = Seq(sft)
        state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
      }
    }
  }

  /**
    * Client-side reduce for the results of a scan. Must have a zero-arg constructor to allow re-creation from
    * a serialized form.
    *
    * Reducers are encouraged to also allow direct instantiation via an alternate constructor, as
    * serialization is generally only used for map/reduce jobs. Similarly, state is encouraged to be
    * lazily evaluated.
    */
  trait FeatureReducer extends SerializableState {

    /**
      * Reduce the results of a scan
      *
      * @param features features
      * @return
      */
    def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature]
  }

  object FeatureReducer {

    /**
      * Serialize a feature reducer
      *
      * @param obj object to serialize
      * @return
      */
    def serialize(obj: FeatureReducer): String = SerializableState.serialize(obj)

    /**
      * Deserialize a feature reducer
      *
      * @param serialized serialized object
      * @return
      */
    def deserialize(serialized: String): FeatureReducer = SerializableState.deserialize(serialized)
  }

  /**
    * Abstract base class for converting the results from a normal feature index
    *
    * @param index index
    * @param sft simple feature type returned from the scan
    * @tparam T result type
    */
  abstract class IndexResultsToFeatures[T](
      protected var index: GeoMesaFeatureIndex[_, _],
      protected var sft: SimpleFeatureType
    ) extends ResultsToFeatures[T] {

    protected var serializer: KryoFeatureSerializer = if (index == null) { null } else { createSerializer }

    override def init(state: Map[String, String]): Unit = {
      val spec = state("spec")
      sft = SimpleFeatureTypes.createType(state("name"), spec)
      index = state.get("isft") match {
        case None => IteratorCache.index(sft, spec, state("idx"))
        case Some(isft) => IteratorCache.index(IteratorCache.sft(isft), isft, state("idx"))
      }
      serializer = createSerializer
    }

    override def state: Map[String, String] = {
      val base = Map(
        "name" -> sft.getTypeName,
        "spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
        "idx"  -> index.identifier
      )
      if (index.sft == sft) { base } else {
        base.updated("isft", SimpleFeatureTypes.encodeType(index.sft, includeUserData = true))
      }
    }

    override def schema: SimpleFeatureType = sft

    protected def createSerializer: KryoFeatureSerializer = {
      val builder = KryoFeatureSerializer.builder(sft)
      if (index.serializedWithId) { builder.withId.build() } else { builder.withoutId.build() }
    }

    def canEqual(other: Any): Boolean = other.isInstanceOf[IndexResultsToFeatures[T]]

    override def equals(other: Any): Boolean = other match {
      case that: IndexResultsToFeatures[T] if that.canEqual(this) =>
        sft == that.sft && {
          if (index == null) { that.index == null } else if (that.index == null) { false } else {
            index.identifier == that.index.identifier && index.sft == that.index.sft
          }
        }

      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(index, sft)
      state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
    }
  }
}
