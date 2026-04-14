/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionRange
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory, StorageMetadata}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class SchemeFilterExtractionTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "dtg:Date,*geom:Point:srid=4326")

  "SchemeFilterExtraction" should {

    "extract temporal filters" in {
      val ecql = ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'")
      val metadata = new TestMetadata(sft, Set("hourly","z2:bits=2"))
      val filters = metadata.getSchemeFilters(ecql)
      filters must haveLength(1)
      filters.head.partitions mustEqual Seq(PartitionRange("hours:attribute=dtg", "80063b40", "80063b58"))
      filters.head.spatialBounds must beEmpty
      filters.head.attributeBounds mustEqual Seq(AttributeOr(0, Seq(AttributeBound("800001564db32000", "8000015652d97c00"))))
    }

    "extract spatio-temporal filters" in {
      val ecql = ECQL.toFilter("bbox(geom,0,0,180,90) AND dtg >= '2018-01-01T00:00:00.000Z' AND dtg < '2018-01-02T00:00:00.000Z'")
      val metadata = new TestMetadata(sft, Set("hourly","z2:bits=2"))
      val filters = metadata.getSchemeFilters(ecql)
      filters must haveLength(1)
      filters.head.partitions must
        containTheSameElementsAs(Seq(PartitionRange("hours:attribute=dtg", "80066ba0", "80066bb8"), PartitionRange("z2:attribute=geom:bits=2", "3", "4")))
      filters.head.spatialBounds mustEqual Seq(SpatialOr(1, Seq(SpatialBound(0, 0, 180, 90))))
      filters.head.attributeBounds mustEqual Seq(AttributeOr(0, Seq(AttributeBound("80000160af049000", "80000160b42aec00"))))
    }
  }

  class TestMetadata(val sft: SimpleFeatureType, names: Set[String])
      extends StorageMetadata with SchemeFilterExtraction with LazyLogging {

    override val schemes: Set[PartitionScheme] = names.map(PartitionSchemeFactory.load(sft, _))

    def getSchemeFilters(filter: Filter): Seq[SchemeFilter] = getFilters(filter)

    override def `type`: String = "test"
    override def addFile(file: StorageMetadata.StorageFile): Unit = throw new UnsupportedOperationException()
    override def removeFile(file: StorageMetadata.StorageFile): Unit = throw new UnsupportedOperationException()
    override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit = throw new UnsupportedOperationException()
    override def getFiles(): Seq[StorageFile] = throw new UnsupportedOperationException()
    override def getFiles(partition: Partition): Seq[StorageFile] = throw new UnsupportedOperationException()
    override def getFiles(filter: Filter): Seq[StorageFile] = throw new UnsupportedOperationException()
    override def close(): Unit = {}
  }
}
