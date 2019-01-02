/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index


import java.nio.charset.StandardCharsets
import java.util.Collections

import org.apache.kudu.ColumnSchema.Encoding
import org.apache.kudu.client.{CreateTableOptions, PartialRow}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.kudu._
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.FeatureIdAdapter
import org.locationtech.geomesa.kudu.utils.ColumnConfiguration
import org.locationtech.geomesa.utils.index.ByteArrays

object IdColumnMapper {
  // use prefix encoding - since this is the first pk column it will be sorted
  private val featureIdAdapter = new FeatureIdAdapter(Encoding.PREFIX_ENCODING, ColumnConfiguration.compression())
}

class IdColumnMapper(index: GeoMesaFeatureIndex[_, _])
    extends KuduColumnMapper(index, Seq(IdColumnMapper.featureIdAdapter)) {

  override def configurePartitions(): CreateTableOptions = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val options = new CreateTableOptions()

    // add hash splits based on our shards
    val shards = index.sft.getIdShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(IdColumnMapper.featureIdAdapter.name), shards)
    }

    options.setRangePartitionColumns(Collections.singletonList(IdColumnMapper.featureIdAdapter.name))

    val idSplits = {
      val upperBound = new String(ByteRange.UnboundedUpperRange, StandardCharsets.UTF_8)
      val configured = DefaultSplitter.Parser.idSplits(splitters)
      if (configured.isEmpty) { Seq("", upperBound) } else {
        // add an upper and lower bound as our splits don't have endpoints
        val builder = Seq.newBuilder[String]
        builder.sizeHint(configured.length + 2)
        builder += ""
        builder ++= configured.sorted
        builder += upperBound
        builder.result.distinct
      }
    }

    idSplits.sliding(2).foreach { case Seq(lo, hi) =>
      val lower = tableSchema.newPartialRow()
      val upper = tableSchema.newPartialRow()
      lower.addString(0, lo)
      upper.addString(0, hi)
      options.addRangePartition(lower, upper)
    }

    options
  }

  override def createKeyValues(value: SingleRowKeyValue[_]): Seq[KuduValue[_]] =
    Seq(KuduValue(new String(value.id, StandardCharsets.UTF_8), IdColumnMapper.featureIdAdapter))

  override def toRowRanges(ranges: Seq[ScanRange[_]],
                           tieredKeyRanges: Seq[ByteRange]): Seq[(Option[PartialRow], Option[PartialRow])] = {

    def lower(id: Array[Byte]): Some[PartialRow] = {
      val row = tableSchema.newPartialRow()
      IdColumnMapper.featureIdAdapter.writeToRow(row, new String(id, StandardCharsets.UTF_8))
      Some(row)
    }

    def upper(id: Array[Byte]): Some[PartialRow] = {
      val row = tableSchema.newPartialRow()
      IdColumnMapper.featureIdAdapter.writeToRow(row, new String(id, StandardCharsets.UTF_8))
      Some(row)
    }

    ranges.asInstanceOf[Seq[ScanRange[Array[Byte]]]].map {
      case SingleRowRange(row)   => (lower(row), upper(row :+ ByteArrays.ZeroByte))
      case BoundedRange(lo, hi)  => (lower(lo), upper(hi))
      case UnboundedRange(_)     => (None, None)
      case LowerBoundedRange(lo) => (lower(lo), None)
      case UpperBoundedRange(hi) => (None, upper(hi))
      case PrefixRange(_)        => (None, None) // not supported
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }
  }
}
