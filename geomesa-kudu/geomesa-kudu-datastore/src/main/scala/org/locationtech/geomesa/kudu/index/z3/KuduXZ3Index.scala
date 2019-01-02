/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index.z3

import org.apache.kudu.Schema
import org.apache.kudu.client.PartialRow
import org.locationtech.geomesa.index.index.IndexKeySpace.{BoundedRange, ScanRange, UnboundedRange}
import org.locationtech.geomesa.index.index.z3._
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, PeriodColumnAdapter, ZColumnAdapter}
import org.opengis.feature.simple.SimpleFeatureType

case object KuduXZ3Index extends KuduXZ3Index

trait KuduXZ3Index extends KuduFeatureIndex[XZ3IndexValues, Z3IndexKey] with KuduZ3Schema[XZ3IndexValues] {

  override val name: String = XZ3Index.Name

  override val version: Int = 1

  override protected val keySpace: XZ3IndexKeySpace = XZ3IndexKeySpace

  override protected def toRowRanges(sft: SimpleFeatureType,
                                     schema: Schema,
                                     range: ScanRange[Z3IndexKey]): (Option[PartialRow], Option[PartialRow]) = {
    def lower(key: Z3IndexKey): Some[PartialRow] = {
      val row = schema.newPartialRow()
      PeriodColumnAdapter.writeToRow(row, key.bin)
      ZColumnAdapter.writeToRow(row, key.z)
      FeatureIdAdapter.writeToRow(row, "")
      Some(row)
    }

    def upper(key: Z3IndexKey): Some[PartialRow] = {
      val row = schema.newPartialRow()
      PeriodColumnAdapter.writeToRow(row, key.bin)
      // note: shouldn't have to worry about overflow, as our xz curves don't use the full 64 bits
      ZColumnAdapter.writeToRow(row, key.z + 1L)
      FeatureIdAdapter.writeToRow(row, "")
      Some(row)
    }

    range match {
      case BoundedRange(lo, hi)  => (lower(lo), upper(hi))
      case UnboundedRange(empty) => (None, None)
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }
}
