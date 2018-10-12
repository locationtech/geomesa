/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index

import java.nio.charset.StandardCharsets

import org.apache.kudu.Schema
import org.apache.kudu.client.{KuduTable, PartialRow}
import org.geotools.factory.Hints
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.kudu.data.KuduQueryPlan.{EmptyPlan, ScanPlan}
import org.locationtech.geomesa.kudu.data.{KuduDataStore, KuduFeature}
import org.locationtech.geomesa.kudu.result.KuduResultAdapter
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.VisibilityAdapter
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.locationtech.geomesa.kudu.{KuduFilterStrategyType, KuduQueryPlanType, KuduValue, WriteOperation}
import org.locationtech.geomesa.security.SecurityUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait KuduTieredFeatureIndex[T, U] extends KuduFeatureIndex[T, U] {

  import scala.collection.JavaConverters._

  /**
    * Tiered key space beyond the primary one, if any
    *
    * @param sft simple feature type
    * @return
    */
  protected def tieredKeySpace(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]]

  protected def createKeyValues(toIndexKey: SimpleFeature => Seq[U],
                                toTieredIndexKey: SimpleFeature => Seq[Array[Byte]])
                               (kf: KuduFeature): Seq[Seq[KuduValue[_]]]

  protected def toTieredRowRanges(sft: SimpleFeatureType,
                                  schema: Schema,
                                  range: ScanRange[U],
                                  tiers: => Seq[ByteRange],
                                  minTier: => Array[Byte],
                                  maxTier: => Array[Byte]): Seq[(Option[PartialRow], Option[PartialRow])]

  override def writer(sft: SimpleFeatureType, ds: KuduDataStore): KuduFeature => Seq[WriteOperation] = {
    // note: table partitioning is disabled in the data store
    val table = ds.client.openTable(getTableNames(sft, ds, None).head)
    val schema = KuduSimpleFeatureSchema(sft)
    val splitters = KuduFeatureIndex.splitters(sft)
    val toIndexKey = keySpace.toIndexKey(sft)
    val toTieredKey = createTieredKey(tieredKeySpace(sft).map(_.toIndexKeyBytes(sft))) _
    createInsert(sft, table, schema, splitters, toIndexKey, toTieredKey)
  }

  override def remover(sft: SimpleFeatureType, ds: KuduDataStore): KuduFeature => Seq[WriteOperation] = {
    // note: table partitioning is disabled in the data store
    val table = ds.client.openTable(getTableNames(sft, ds, None).head)
    val toIndexKey = keySpace.toIndexKey(sft)
    val toTieredKey = createTieredKey(tieredKeySpace(sft).map(_.toIndexKeyBytes(sft))) _
    createDelete(table, toIndexKey, toTieredKey)
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: KuduDataStore,
                            filter: KuduFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): KuduQueryPlanType = {

    val tier = tieredKeySpace(sft).orNull.asInstanceOf[IndexKeySpace[Any, Any]]
    val primary = filter.primary.orNull
    val secondary = filter.secondary.orNull
    // only evaluate the tiered ranges if needed - depending on the primary filter we might not use them
    lazy val tiers = tier.getRangeBytes(tier.getRanges(tier.getIndexValues(sft, secondary, explain))).toSeq

    if (tier == null || primary == null || secondary == null || tiers.isEmpty) {
      // primary == null handles Filter.INCLUDE
      super.getQueryPlan(sft, ds, filter, hints, explain)
    } else {
      val values = keySpace.getIndexValues(sft, primary, explain)
      val keyRanges = keySpace.getRanges(values)

      lazy val minTier = ByteRange.min(tiers)
      lazy val maxTier = ByteRange.max(tiers)

      val kuduSchema = tableSchema(sft)
      val ranges = keyRanges.flatMap(toTieredRowRanges(sft, kuduSchema, _, tiers, minTier, maxTier))

      if (ranges.isEmpty) { EmptyPlan(filter) } else {
        val schema = KuduSimpleFeatureSchema(sft)

        val fullFilter =
          if (keySpace.useFullFilter(Some(values), Some(ds.config), hints)) { filter.filter } else { filter.secondary }

        val auths = ds.config.authProvider.getAuthorizations.asScala.map(_.getBytes(StandardCharsets.UTF_8))

        val KuduFilter(predicates, ecql) = fullFilter.map(schema.predicate).getOrElse(KuduFilter(Seq.empty, None))

        val adapter = KuduResultAdapter(sft, auths, ecql, hints)

        val tables = getTablesForQuery(sft, ds, filter.filter)

        ScanPlan(filter, tables, ranges.toSeq, predicates, ecql, adapter, ds.config.queryThreads)
      }
    }
  }

  private def createInsert(sft: SimpleFeatureType,
                           table: KuduTable,
                           schema: KuduSimpleFeatureSchema,
                           splitters: Map[String, String],
                           toIndexKey: SimpleFeature => Seq[U],
                           toTieredIndexKey: SimpleFeature => Seq[Array[Byte]])
                          (kf: KuduFeature): Seq[WriteOperation] = {
    val featureValues = schema.serialize(kf.feature)
    val vis = SecurityUtils.getVisibility(kf.feature)
    val partitioning = () => createPartition(sft, table, splitters, kf.bin)
    createKeyValues(toIndexKey, toTieredIndexKey)(kf).map { key =>
      val upsert = table.newUpsert()
      val row = upsert.getRow
      key.foreach(_.writeToRow(row))
      featureValues.foreach(_.writeToRow(row))
      VisibilityAdapter.writeToRow(row, vis)
      WriteOperation(upsert, s"$identifier.${kf.bin}", partitioning)
    }
  }

  private def createDelete(table: KuduTable,
                           toIndexKey: SimpleFeature => Seq[U],
                           toTieredIndexKey: SimpleFeature => Seq[Array[Byte]])
                          (kf: KuduFeature): Seq[WriteOperation] = {
    createKeyValues(toIndexKey, toTieredIndexKey)(kf).map { key =>
      val delete = table.newDelete()
      val row = delete.getRow
      key.foreach(_.writeToRow(row))
      WriteOperation(delete, "", null)
    }
  }

  private def createTieredKey(tieredBytes: Option[ToIndexKeyBytes])(feature: SimpleFeature): Seq[Array[Byte]] =
    tieredBytes.map(_.apply(Seq.empty, feature, Array.empty)).getOrElse(Seq(Array.empty))
}
