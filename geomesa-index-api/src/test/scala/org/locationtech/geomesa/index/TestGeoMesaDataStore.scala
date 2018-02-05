/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import java.util.Comparator

import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.locationtech.geomesa.index.TestGeoMesaDataStore.{TestWrappedFeature, TestWrite, _}
import org.locationtech.geomesa.index.api.{GeoMesaIndexManager, _}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.geotools.{GeoMesaAppendFeatureWriter, GeoMesaDataStore, GeoMesaFeatureWriter, GeoMesaModifyFeatureWriter}
import org.locationtech.geomesa.index.index._
import org.locationtech.geomesa.index.index.legacy.AttributeDateIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.index.stats._
import org.locationtech.geomesa.index.utils.{Explainer, LocalLocking}
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.stats.{SeqStat, Stat}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class TestGeoMesaDataStore(looseBBox: Boolean)
    extends GeoMesaDataStore[TestGeoMesaDataStore, TestWrappedFeature, TestWrite](TestConfig(looseBBox))
    with LocalLocking {

  override val metadata: GeoMesaMetadata[String] = new InMemoryMetadata[String]

  override val stats: GeoMesaStats = new TestStats(this)

  override val manager: TestIndexManager = new TestIndexManager

  override protected def createFeatureWriterAppend(sft: SimpleFeatureType,
                                                   indices: Option[Seq[TestFeatureIndexType]]): TestFeatureWriterType =
    new TestAppendFeatureWriter(sft, this, indices)

  override protected def createFeatureWriterModify(sft: SimpleFeatureType,
                                                   indices: Option[Seq[TestFeatureIndexType]],
                                                   filter: Filter): TestFeatureWriterType =
    new TestModifyFeatureWriter(sft, this, indices, filter)

  override def delete(): Unit = throw new NotImplementedError()
}

object TestGeoMesaDataStore {

  type TestFeatureIndexType = GeoMesaFeatureIndex[TestGeoMesaDataStore, TestWrappedFeature, TestWrite]
  type TestFeatureWriterType = GeoMesaFeatureWriter[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestFeatureIndex]
  type TestAppendFeatureWriterType = GeoMesaAppendFeatureWriter[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestFeatureIndex]
  type TestModifyFeatureWriterType = GeoMesaModifyFeatureWriter[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestFeatureIndex]
  type TestIndexManagerType = GeoMesaIndexManager[TestGeoMesaDataStore, TestWrappedFeature, TestWrite]
  type TestQueryPlanType = QueryPlan[TestGeoMesaDataStore, TestWrappedFeature, TestWrite]
  type TestFilterStrategyType = FilterStrategy[TestGeoMesaDataStore, TestWrappedFeature, TestWrite]

  val ByteComparator = new Comparator[Array[Byte]] {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val minLength = if (o1.length < o2.length) { o1.length } else { o2.length }
      var i = 0
      while (i < minLength) {
        if (o1(i) != o2(i)) {
          return (o1(i) & 0xff) - (o2(i) & 0xff)
        }
        i += 1
      }
      o1.length - o2.length
    }
  }

  case class TestWrappedFeature(feature: SimpleFeature) extends WrappedFeature

  case class TestWrite(row: Array[Byte], feature: SimpleFeature, delete: Boolean = false)

  case class TestRange(start: Array[Byte], end: Array[Byte]) {
    override def toString: String = s"TestRange(${start.mkString(":")}, ${end.mkString(":")}}"
  }

  case class TestScanConfig(ranges: Seq[TestRange], ecql: Option[Filter])

  case class TestConfig(looseBBox: Boolean) extends GeoMesaDataStoreConfig {
    override val catalog: String = "test"
    override val audit: Option[(AuditWriter, AuditProvider, String)] = None
    override val generateStats: Boolean = true
    override val queryThreads: Int = 1
    override val queryTimeout: Option[Long] = None
    override val caching: Boolean = false
    override val namespace: Option[String] = None
  }

  class TestIndexManager extends GeoMesaIndexManager[TestGeoMesaDataStore, TestWrappedFeature, TestWrite] {
    override val CurrentIndices: Seq[TestFeatureIndex] =
      Seq(new TestZ3Index, new TestZ2Index, new TestIdIndex, new TestAttributeIndex)
    override val AllIndices: Seq[TestFeatureIndex] = CurrentIndices :+ new TestAttributeDateIndex
    override def lookup: Map[(String, Int), TestFeatureIndex] =
      super.lookup.asInstanceOf[Map[(String, Int), TestFeatureIndex]]
    override def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[TestFeatureIndex] =
      super.indices(sft, mode).asInstanceOf[Seq[TestFeatureIndex]]
    override def index(identifier: String): TestFeatureIndex = super.index(identifier).asInstanceOf[TestFeatureIndex]
  }

  class TestZ3Index extends TestFeatureIndex
      with Z3Index[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestRange, TestScanConfig]

  class TestZ2Index extends TestFeatureIndex
      with Z2Index[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestRange, TestScanConfig]

  class TestIdIndex extends TestFeatureIndex
      with IdIndex[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestRange, TestScanConfig]

  class TestAttributeIndex extends TestFeatureIndex
      with AttributeIndex[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestRange, TestScanConfig] {
    override val version: Int = 2
  }

  class TestAttributeDateIndex extends TestFeatureIndex
     with AttributeDateIndex[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestRange, TestScanConfig]

  trait TestFeatureIndex extends TestFeatureIndexType
      with IndexAdapter[TestGeoMesaDataStore, TestWrappedFeature, TestWrite, TestRange, TestScanConfig] {

    private val ordering = new Ordering[(Array[Byte], SimpleFeature)] {
      override def compare(x: (Array[Byte], SimpleFeature), y: (Array[Byte], SimpleFeature)): Int =
        ByteComparator.compare(x._1, y._1)
    }

    val features = scala.collection.mutable.SortedSet.empty[(Array[Byte], SimpleFeature)](ordering)

    override val version = 1

    override def removeAll(sft: SimpleFeatureType, ds: TestGeoMesaDataStore): Unit = features.clear()

    override def delete(sft: SimpleFeatureType, ds: TestGeoMesaDataStore, shared: Boolean): Unit = features.clear()

    override protected def createInsert(row: Array[Byte], feature: TestWrappedFeature): TestWrite =
      TestWrite(row, feature.feature)

    override protected def createDelete(row: Array[Byte], feature: TestWrappedFeature): TestWrite =
      TestWrite(row, feature.feature, delete = true)

    override protected def range(start: Array[Byte], end: Array[Byte]): TestRange = TestRange(start, end)

    override protected def rangeExact(row: Array[Byte]): TestRange = TestRange(row, IndexAdapter.rowFollowingRow(row))

    override protected def scanConfig(sft: SimpleFeatureType,
                                      ds: TestGeoMesaDataStore,
                                      filter: TestFilterStrategyType,
                                      ranges: Seq[TestRange],
                                      ecql: Option[Filter],
                                      hints: Hints): TestScanConfig = TestScanConfig(ranges, ecql)

    override protected def scanPlan(sft: SimpleFeatureType,
                                    ds: TestGeoMesaDataStore,
                                    filter: TestFilterStrategyType,
                                    config: TestScanConfig): TestQueryPlanType = TestQueryPlan(this, filter, config.ranges, config.ecql)

    override def toString: String = getClass.getSimpleName
  }

  class TestAppendFeatureWriter(sft: SimpleFeatureType,
                                ds: TestGeoMesaDataStore,
                                indices: Option[Seq[TestFeatureIndexType]])
      extends TestFeatureWriterType(sft, ds, indices) with TestAppendFeatureWriterType with TestFeatureWriter

  class TestModifyFeatureWriter(sft: SimpleFeatureType,
                                ds: TestGeoMesaDataStore,
                                indices: Option[Seq[TestFeatureIndexType]],
                                val filter: Filter)
      extends TestFeatureWriterType(sft, ds, indices) with TestModifyFeatureWriterType with TestFeatureWriter

  trait TestFeatureWriter extends TestFeatureWriterType {

    override protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[TestFeatureIndex] =
      tables.map(t => ds.manager.indices(sft, IndexMode.Write).find(_.getTableName(sft.getTypeName, ds) == t).orNull)

    override protected def executeWrite(mutator: TestFeatureIndex, writes: Seq[TestWrite]): Unit = {
      writes.foreach { case TestWrite(row, feature, _) => mutator.features.add((row, feature)) }
    }

    override protected def executeRemove(mutator: TestFeatureIndex, removes: Seq[TestWrite]): Unit =
      removes.foreach { case TestWrite(row, feature, _) => mutator.features.remove((row, feature)) }

    override def wrapFeature(feature: SimpleFeature): TestWrappedFeature = TestWrappedFeature(feature)
  }

  case class TestQueryPlan(index: TestFeatureIndex,
                           filter: TestFilterStrategyType,
                           ranges: Seq[TestRange],
                           ecql: Option[Filter]) extends TestQueryPlanType {
    override def scan(ds: TestGeoMesaDataStore): CloseableIterator[SimpleFeature] = {
      def contained(range: TestRange, row: Array[Byte]): Boolean =
        ByteComparator.compare(range.start, row) <= 0 && ByteComparator.compare(range.end, row) > 0
      index.features.toIterator.collect {
        case (row, sf) if ranges.exists(contained(_, row)) && ecql.forall(_.evaluate(sf)) => sf
      }
    }

    override def explain(explainer: Explainer, prefix: String): Unit = {
      explainer(s"ranges (${ranges.length}): ${ranges.take(5).map(r => s"[${r.start.mkString("")}:${r.end.mkString("")})")}")
      explainer(s"ecql: ${ecql.map(org.locationtech.geomesa.filter.filterToString).getOrElse("INCLUDE")}")
    }
  }

  class TestStats(override protected val ds: TestGeoMesaDataStore) extends MetadataBackedStats {

    override private [geomesa] val metadata = new InMemoryMetadata[Stat]

    override protected val generateStats = true

    override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
      val stat = Stat(sft, stats)
      SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).foreach(stat.observe)
      stat match {
        case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
        case s: T => Seq(s)
      }
    }
  }
}