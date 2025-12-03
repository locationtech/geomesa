/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.filter

import org.apache.hadoop.hbase.filter.FilterBase
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.filters.{S2Filter, S3Filter, Z2Filter, Z3Filter}
import org.locationtech.geomesa.index.index.s2.{S2Index, S2IndexValues}
import org.locationtech.geomesa.index.index.s3.{S3Index, S3IndexValues}
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexValues}
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexValues}

object IndexFilters {

  /**
   * Configure z/s curve filters, based on the index
   *
   * @param index feature index
   * @param values query values
   * @return
   */
  def apply(index: GeoMesaFeatureIndex[_, _], values: Any): Option[(Int, FilterBase)] = {
    // TODO pull this out to be SPI loaded so that new indices can be added seamlessly
    index match {
      case _: Z3Index =>
        Some(Z3HBaseFilter.Priority, Z3HBaseFilter(Z3Filter(values.asInstanceOf[Z3IndexValues]), index.keySpace.sharding.length))

      case _: Z2Index =>
        Some(Z2HBaseFilter.Priority, Z2HBaseFilter(Z2Filter(values.asInstanceOf[Z2IndexValues]), index.keySpace.sharding.length))

      case _: S2Index =>
        Some(S2HBaseFilter.Priority, S2HBaseFilter(S2Filter(values.asInstanceOf[S2IndexValues]), index.keySpace.sharding.length))

      case _: S3Index =>
        Some(S3HBaseFilter.Priority, S3HBaseFilter(S3Filter(values.asInstanceOf[S3IndexValues]), index.keySpace.sharding.length))

      // TODO GEOMESA-1807 deal with non-points in a pushdown XZ filter
      case _ => None
    }
  }
}
