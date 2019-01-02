/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.coprocessor.aggregators._
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.data.{EmptyPlan, HBaseDataStore, HBaseFeature, HBaseQueryPlan}
import org.locationtech.geomesa.hbase.filters.CqlTransformFilter
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.hbase.{HBaseFeatureIndexType, HBaseFilterStrategyType, HBaseQueryPlanType, HBaseSystemProperties}
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait HBaseIndexAdapter extends HBaseFeatureIndexType
    with IndexAdapter[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig]
    with ClientSideFiltering[Result] with LazyLogging {

  override def rowAndValue(result: Result): RowAndValue = {
    val cell = result.rawCells()(0)
    RowAndValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }

  override protected def createInsert(row: Array[Byte], feature: HBaseFeature): Mutation = {
    val put = new Put(row)
    feature.values.foreach(v => put.addImmutable(v.cf, v.cq, v.value))
    feature.visibility.foreach(put.setCellVisibility)
    put.setDurability(HBaseIndexAdapter.durability)
  }

  override protected def createDelete(row: Array[Byte], feature: HBaseFeature): Mutation = {
    val del = new Delete(row)
    feature.values.foreach(v => del.addFamily(v.cf))
    feature.visibility.foreach(del.setCellVisibility)
    del
  }

  override protected def createRange(start: Array[Byte], end: Array[Byte]): Scan =
    new Scan(start, end)

  override protected def createRange(row: Array[Byte]): Scan =
    new Scan(row, ByteArrays.rowFollowingRow(row)).setSmall(true)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: HBaseFilterStrategyType,
                                  config: ScanConfig): HBaseQueryPlanType = {
    if (config.ranges.isEmpty) { EmptyPlan(filter) } else {
      val tables = getTablesForQuery(sft, ds, filter.filter).map(TableName.valueOf)
      val ScanConfig(ranges, colFamily, hbaseFilters, coprocessor, toFeatures) = config
      buildPlatformScanPlan(ds, sft, filter, ranges, colFamily, tables, hbaseFilters, coprocessor, toFeatures)
    }
  }

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: HBaseDataStore,
                                    filter: HBaseFilterStrategyType,
                                    ranges: Seq[Scan],
                                    ecql: Option[Filter],
                                    hints: Hints): ScanConfig = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val transform: Option[(String, SimpleFeatureType)] = hints.getTransform

    val (colFamily, schema) = transform match {
      case None => (HBaseColumnGroups.default, sft)
      case Some((tdefs, _)) => HBaseColumnGroups.group(sft, tdefs, ecql)
    }

    if (!ds.config.remoteFilter) {
      // everything is done client side
      ScanConfig(ranges, colFamily, Seq.empty, None, resultsToFeatures(schema, ecql, transform))
    } else {
      val returnSchema = transform.map(_._2).getOrElse(schema)

      // TODO not actually used for coprocessors
      val toFeatures = resultsToFeatures(schema, returnSchema)

      val coprocessorConfig = if (hints.isDensityQuery) {
        val options = HBaseDensityAggregator.configure(schema, this, ecql, hints)
        Some(CoprocessorConfig(options, HBaseDensityAggregator.bytesToFeatures))
      } else if (hints.isArrowQuery) {
        val (options, reduce) = HBaseArrowAggregator.configure(schema, this, ds.stats, filter.filter, ecql, hints)
        Some(CoprocessorConfig(options, HBaseArrowAggregator.bytesToFeatures, reduce))
      } else if (hints.isStatsQuery) {
        val options = HBaseStatsAggregator.configure(schema, filter.index, ecql, hints)
        val reduce = StatsScan.reduceFeatures(returnSchema, hints)(_)
        Some(CoprocessorConfig(options, HBaseStatsAggregator.bytesToFeatures, reduce))
      } else if (hints.isBinQuery) {
        val options = HBaseBinAggregator.configure(schema, filter.index, ecql, hints)
        Some(CoprocessorConfig(options, HBaseBinAggregator.bytesToFeatures))
      } else {
        None
      }

      // if there is a coprocessorConfig it handles filter/transform
      val filters = if (coprocessorConfig.isDefined || (ecql.isEmpty && transform.isEmpty)) {
        Seq.empty
      } else {
        Seq((CqlTransformFilter.Priority, CqlTransformFilter(schema, ecql, transform)))
      }

      ScanConfig(ranges, colFamily, filters, coprocessorConfig, toFeatures)
    }
  }

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false

  protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                      sft: SimpleFeatureType,
                                      filter: HBaseFilterStrategyType,
                                      ranges: Seq[Scan],
                                      colFamily: Array[Byte],
                                      tables: Seq[TableName],
                                      hbaseFilters: Seq[(Int, HFilter)],
                                      coprocessor: Option[CoprocessorConfig],
                                      toFeatures: Iterator[Result] => Iterator[SimpleFeature]): HBaseQueryPlan

  /**
    * Turns hbase results into simple features
    *
    * @param sft simple feature type
    * @param returnSft return simple feature type (transform, etc)
    * @return
    */
  private [hbase] def resultsToFeatures(sft: SimpleFeatureType,
                                        returnSft: SimpleFeatureType): Iterator[Result] => Iterator[SimpleFeature] = {
    // Perform a projecting decode of the simple feature
    val getId = getIdFromRow(sft)
    val deserializer = KryoFeatureSerializer(returnSft, SerializationOptions.withoutId)
    resultsToFeatures(deserializer, getId)
  }

  private def resultsToFeatures(deserializer: KryoFeatureSerializer,
                                getId: (Array[Byte], Int, Int, SimpleFeature) => String)
                               (results: Iterator[Result]): Iterator[SimpleFeature] = {
    results.map { result =>
      val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
      val sf = deserializer.deserialize(value, valueOffset, valueLength)
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row, rowOffset, rowLength, sf))
      sf
    }
  }
}

object HBaseIndexAdapter extends LazyLogging {

  case class ScanConfig(ranges: Seq[Scan],
                        colFamily: Array[Byte],
                        filters: Seq[(Int, HFilter)],
                        coprocessor: Option[CoprocessorConfig],
                        entriesToFeatures: Iterator[Result] => Iterator[SimpleFeature])

  val durability: Durability = HBaseSystemProperties.WalDurability.option match {
    case Some(value) =>
      Durability.values.find(_.toString.equalsIgnoreCase(value)).getOrElse{
        logger.error(s"Invalid HBase WAL durability setting: $value. Falling back to default durability")
        Durability.USE_DEFAULT
      }
    case None => Durability.USE_DEFAULT
  }
}
