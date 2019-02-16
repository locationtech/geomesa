/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, JoinFunction, JoinPlan}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloIndexAdapter, AccumuloQueryPlan}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter.{FilterHelper, andOption, partitionPrimarySpatials, partitionPrimaryTemporals}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexValues}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.LocalQueryRunner.ArrowDictionaryHook
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.{ByteArrays, IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.stats.{Cardinality, Stat}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.Try

/**
  * Mixin trait to add join support to the normal attribute index class
  */
trait AccumuloJoinIndex extends GeoMesaFeatureIndex[AttributeIndexValues[Any], AttributeIndexKey] {

  this: AttributeIndex =>

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  protected val attributeIndex: Int = sft.indexOf(attribute)
  protected val descriptor: AttributeDescriptor = sft.getDescriptor(attributeIndex)
  protected val binding: Class[_] = descriptor.getType.getBinding
  protected val indexSft: SimpleFeatureType = IndexValueEncoder.getIndexSft(sft)

  override val name: String = JoinIndex.name
  override val identifier: String = GeoMesaFeatureIndex.identifier(name, version, attributes)

  abstract override def getFilterStrategy(filter: Filter,
                                          transform: Option[SimpleFeatureType],
                                          stats: Option[GeoMesaStats]): Option[FilterStrategy] = {
    super.getFilterStrategy(filter, transform, stats).flatMap { strategy =>
      val join = requiresJoin(strategy.secondary, transform)
      // verify that it's ok to return join plans, and filter them out if not
      if (join && !JoinIndex.AllowJoinPlans.get) { None } else {
        lazy val cost = strategy.primary match {
          case None => Long.MaxValue
          case Some(f) =>
            val statCost = for { stats <- stats; count <- stats.getCount(sft, f, exact = false) } yield {
              val hi = descriptor.getCardinality == Cardinality.HIGH
              if (join && !hi) {
                count * 10 // de-prioritize join queries, they are much more expensive
              } else if (hi && !join) {
                count / 10 // prioritize attributes marked high-cardinality
              } else {
                count
              }
            }
            statCost.getOrElse(indexBasedCost(f, join))
        }
        Some(FilterStrategy(strategy.index, strategy.primary, strategy.secondary, cost))
      }
    }
  }

  /**
    * Create a query plan against a join index - if possible, will use the reduced index-values to scan
    * the single table, otherwise will require a join against the id index
    *
    * @param filter filter strategy
    * @param tables tables to scan
    * @param ranges ranges to scan
    * @param colFamily column family to scan, optional
    * @param schema simple feature schema being scanned
    * @param ecql secondary push-down predicates
    * @param hints query hints
    * @param numThreads query threads
    * @return
    */
  def createQueryPlan(filter: FilterStrategy,
                      tables: Seq[String],
                      ranges: Seq[org.apache.accumulo.core.data.Range],
                      colFamily: Option[Text],
                      schema: SimpleFeatureType,
                      ecql: Option[Filter],
                      hints: Hints,
                      numThreads: Int): AccumuloQueryPlan = {

    // for queries that don't require a join, creates a regular batch scan plan
    def plan(iters: Seq[IteratorSetting],
             kvsToFeatures: java.util.Map.Entry[Key, Value] => SimpleFeature,
             reduce: Option[CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]]) =
      BatchScanPlan(filter, tables, ranges, iters, colFamily, kvsToFeatures, reduce, numThreads)

    val transform = hints.getTransformSchema

    if (hints.isBinQuery) {
      // check to see if we can execute against the index values
      if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
          hints.getBinGeomField.forall(indexSft.indexOf(_) != -1) &&
          hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
          supportsFilter(ecql)) {
        val iter = BinAggregatingIterator.configure(indexSft, this, ecql, hints)
        val iters = visibilityIter(indexSft) :+ iter
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        plan(iters, kvsToFeatures, None)
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (hints.isArrowQuery) {
      // check to see if we can execute against the index values
      if (canUseIndexSchema(ecql, transform)) {
        val (iter, reduce) = ArrowIterator.configure(indexSft, this, ds.stats, filter.filter, ecql, hints)
        val iters = visibilityIter(indexSft) :+ iter
        plan(iters, ArrowIterator.kvsToFeatures(), Some(reduce))
      } else if (canUseIndexSchemaPlusKey(ecql, transform)) {
        val transformSft = transform.getOrElse {
          throw new IllegalStateException("Must have a transform for attribute key plus value scan")
        }
        // first filter and apply the transform
        val filterTransformIter = FilterTransformIterator.configure(indexSft, this, ecql, hints, 23).get
        // clear the transforms as we've already accounted for them
        hints.clearTransforms()
        // next add the attribute value from the row key
        val rowValueIter = AttributeKeyValueIterator.configure(this, transformSft, 24)
        // finally apply the arrow iterator on the resulting features
        val (iter, reduce) = ArrowIterator.configure(transformSft, this, ds.stats, None, None, hints)
        val iters = visibilityIter(indexSft) ++ Seq(filterTransformIter, rowValueIter, iter)
        plan(iters, ArrowIterator.kvsToFeatures(), Some(reduce))
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (hints.isDensityQuery) {
      // check to see if we can execute against the index values
      val weightIsAttribute = hints.getDensityWeight.contains(attribute)
      if (supportsFilter(ecql) && (weightIsAttribute || hints.getDensityWeight.forall(indexSft.indexOf(_) != -1))) {
        val visIter = visibilityIter(indexSft)
        val iters = if (weightIsAttribute) {
          // create a transform sft with the attribute added
          val transform = {
            val builder = new SimpleFeatureTypeBuilder()
            builder.setNamespaceURI(null: String)
            builder.setName(indexSft.getTypeName + "--attr")
            builder.setAttributes(indexSft.getAttributeDescriptors)
            builder.add(sft.getDescriptor(attribute))
            if (indexSft.getGeometryDescriptor != null) {
              builder.setDefaultGeometry(indexSft.getGeometryDescriptor.getLocalName)
            }
            builder.setCRS(indexSft.getCoordinateReferenceSystem)
            val tmp = builder.buildFeatureType()
            tmp.getUserData.putAll(indexSft.getUserData)
            tmp
          }
          // priority needs to be between vis iter (21) and density iter (25)
          val keyValueIter = AttributeKeyValueIterator.configure(this, transform, 23)
          val densityIter = DensityIterator.configure(transform, this, ecql, hints)
          visIter :+ keyValueIter :+ densityIter
        } else {
          visIter :+ DensityIterator.configure(indexSft, this, ecql, hints)
        }
        val kvsToFeatures = DensityIterator.kvsToFeatures()
        plan(iters, kvsToFeatures, None)
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (hints.isStatsQuery) {
      // check to see if we can execute against the index values
      if (Try(Stat(indexSft, hints.getStatsQuery)).isSuccess && supportsFilter(ecql)) {
        val iter = StatsIterator.configure(indexSft, this, ecql, hints)
        val iters = visibilityIter(indexSft) :+ iter
        val kvsToFeatures = StatsIterator.kvsToFeatures()
        val reduce = Some(StatsScan.reduceFeatures(indexSft, hints)(_))
        plan(iters, kvsToFeatures, reduce)
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (canUseIndexSchema(ecql, transform)) {
      // we can use the index value
      // transform has to be non-empty to get here and can only include items
      // in the index value (not the index keys aka the attribute indexed)
      val transformSft = transform.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute value scan")
      }
      val iter = FilterTransformIterator.configure(indexSft, this, ecql, hints.getTransform, hints.getSampling)
      // add the attribute-level vis iterator if necessary
      val iters = visibilityIter(schema) ++ iter.toSeq
      // need to use transform to convert key/values
      val kvsToFeatures = AccumuloIndexAdapter.entriesToFeatures(this, transformSft)
      plan(iters, kvsToFeatures, None)
    } else if (canUseIndexSchemaPlusKey(ecql, transform)) {
      // we can use the index PLUS the value
      val transformSft = transform.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute key plus value scan")
      }
      val iter = FilterTransformIterator.configure(indexSft, this, ecql, hints.getTransform, hints.getSampling)
      // add the attribute-level vis iterator if necessary
      val iters = visibilityIter(schema) ++ iter.toSeq :+ AttributeKeyValueIterator.configure(this, transformSft)
      // need to use transform to convert key/values
      val kvsToFeatures = AccumuloIndexAdapter.entriesToFeatures(this, transformSft)
      plan(iters, kvsToFeatures, None)
    } else {
      // have to do a join against the record table
      createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
    }
  }

  /**
    * Gets a query plan comprised of a join against the record table. This is the slowest way to
    * execute a query, so we avoid it if possible.
    */
  private def createJoinPlan(filter: FilterStrategy,
                             tables: Seq[String],
                             ranges: Seq[org.apache.accumulo.core.data.Range],
                             colFamily: Option[Text],
                             schema: SimpleFeatureType,
                             ecql: Option[Filter],
                             hints: Hints,
                             numThreads: Int): AccumuloQueryPlan = {
    import org.locationtech.geomesa.filter.ff
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // apply any secondary filters or transforms against the record table
    val recordIndex = ds.manager.indices(sft, IndexMode.Read).find(_.name == IdIndex.name).getOrElse {
      throw new RuntimeException("Id index does not exist for join query: " +
          ds.manager.indices(sft, IndexMode.Read).map(_.identifier).mkString(", "))
    }

    // break out the st filter to evaluate against the attribute table
    val (stFilter, ecqlFilter) = ecql.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, sft)
      (andOption(geomFilters ++ temporalFilters), andOption(nonSTFilters))
    }.getOrElse((None, None))

    val (recordColFamily, recordSchema) = {
      val (cf, s) = ds.adapter.groups.group(sft, hints.getTransformDefinition, ecqlFilter)
      (Some(new Text(AccumuloIndexAdapter.mapColumnFamily(recordIndex)(cf))), s)
    }

    // since each range is a single row, it wouldn't be very efficient to do any aggregating scans
    // instead, handle them with the local query runner
    val resultSft = hints.getTransformSchema.getOrElse(sft)
    val recordIterators = {
      val recordIter = FilterTransformIterator.configure(recordSchema, recordIndex, ecqlFilter, hints).toSeq
      if (sft.getVisibilityLevel != VisibilityLevel.Attribute) { recordIter } else {
        Seq(KryoVisibilityRowEncoder.configure(recordSchema)) ++ recordIter
      }
    }
    val kvsToFeatures = AccumuloIndexAdapter.entriesToFeatures(recordIndex, resultSft)
    val reduce: CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {
      val hook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
      LocalQueryRunner.transform(resultSft, _, None, hints, hook)
    }

    val recordTables = recordIndex.getTablesForQuery(filter.filter)
    val recordThreads = ds.asInstanceOf[AccumuloDataStore].config.recordThreads

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val joinFunction: JoinFunction = {
      val prefix = sft.getTableSharingBytes
      val idToBytes = GeoMesaFeatureIndex.idToBytes(sft)
      kv => {
        val row = kv.getKey.getRow
        new Range(new Text(ByteArrays.concat(prefix, idToBytes(getIdFromRow(row.getBytes, 0, row.getLength, null)))))
      }
    }

    val joinQuery = BatchScanPlan(filter, recordTables, Seq.empty, recordIterators, recordColFamily, kvsToFeatures,
      Some(reduce), recordThreads)

    val attributeIters = visibilityIter(indexSft) ++
        FilterTransformIterator.configure(indexSft, this, stFilter, None, hints.getSampling).toSeq

    JoinPlan(filter, tables, ranges, attributeIters, colFamily, recordThreads, joinFunction, joinQuery)
  }

  private def visibilityIter(schema: SimpleFeatureType): Seq[IteratorSetting] = sft.getVisibilityLevel match {
    case VisibilityLevel.Feature   => Seq.empty
    case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(schema))
  }

  /**
    * full index equals query:
    *   high cardinality - 10
    *   unknown cardinality - 101
    * full index range query:
    *   high cardinality - 100
    *   unknown cardinality - 1010
    * full index not null query:
    *   high cardinality - 500
    *   unknown cardinality - 5050
    * join index equals query:
    *   high cardinality - 100
    *   unknown cardinality - 1010
    * join index range query:
    *   high cardinality - 1000
    *   unknown cardinality - 10100
    * join index not null query:
    *   high cardinality - 5000
    *   unknown cardinality - 50500
    *
    * Compare with id lookups at 1, z2/z3 at 200-401
    */
  private def indexBasedCost(primary: Filter, join: Boolean): Long = {
    import FilterHelper.extractAttributeBounds

    val cost = Option(extractAttributeBounds(primary, attribute, binding)).filter(_.nonEmpty).map { bounds =>
      if (bounds.disjoint) { 0L } else {
        // high cardinality attributes and equality queries are prioritized
        // joins and not-null queries are de-prioritized

        val baseCost = descriptor.getCardinality() match {
          case Cardinality.HIGH    => 10
          case Cardinality.UNKNOWN => 101
          case Cardinality.LOW     => 1000
        }
        val secondaryIndexMultiplier = if (!bounds.forall(_.isBounded)) {
          50 // not null
        } else if (bounds.precise && !bounds.exists(_.isRange)) {
          1 // equals
        } else {
          10 // range
        }
        val joinMultiplier = if (join) { 10 + (bounds.values.length - 1) } else { 1 }

        baseCost * secondaryIndexMultiplier * joinMultiplier
      }
    }
    cost.getOrElse(Long.MaxValue)
  }

  /**
    * Does the query require a join against the record table, or can it be satisfied
    * in a single scan
    *
    * @param filter non-attribute filter being evaluated, if any
    * @param transform transform being applied, if any
    * @return
    */
  private def requiresJoin(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean =
    !canUseIndexSchema(filter, transform) && !canUseIndexSchemaPlusKey(filter, transform)

  /**
    * Determines if the given filter and transform can operate on index encoded values.
    */
  private def canUseIndexSchema(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean = {
    // verify that transform *does* exist and only contains fields in the index sft,
    // and that filter *does not* exist or can be fulfilled by the index sft
    supportsTransform(transform) && supportsFilter(filter)
  }

  /**
    * Determines if the given filter and transform can operate on index encoded values
    * in addition to the values actually encoded in the attribute index keys
    */
  private def canUseIndexSchemaPlusKey(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean = {
    transform.exists { t =>
      val attributes = t.getAttributeDescriptors.asScala.map(_.getLocalName)
      attributes.forall(a => a == attribute || indexSft.indexOf(a) != -1) && supportsFilter(filter)
    }
  }

  private def supportsTransform(transform: Option[SimpleFeatureType]): Boolean =
    transform.exists(_.getAttributeDescriptors.asScala.map(_.getLocalName).forall(indexSft.indexOf(_) != -1))

  private def supportsFilter(filter: Option[Filter]): Boolean =
    filter.forall(FilterHelper.propertyNames(_, sft).forall(indexSft.indexOf(_) != -1))
}
