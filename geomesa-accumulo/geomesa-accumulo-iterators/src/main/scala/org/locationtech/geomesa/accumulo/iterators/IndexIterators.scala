/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.FilterCompatibility.FilterCompatibility
import org.locationtech.geomesa.index.index.s2.{S2Index, S2IndexValues}
import org.locationtech.geomesa.index.index.s3.{S3Index, S3IndexValues}
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexValues}
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexValues}

object IndexIterators {

  /**
   * Configure z/s curve iterators, based on the index
   *
   * @param index feature index
   * @param values query values
   * @return
   */
  def configure(
      index: GeoMesaFeatureIndex[_, _],
      values: Any,
      priority: Int,
      compatibility: Option[FilterCompatibility] = None): Option[IteratorSetting] = {
    // TODO pull this out to be SPI loaded so that new indices can be added seamlessly
    if (index.name == Z3Index.name) {
      val offset = index.keySpace.sharding.length + index.keySpace.sharing.length
      Some(Z3Iterator.configure(values.asInstanceOf[Z3IndexValues], offset, compatibility, priority))
    } else if (index.name == Z2Index.name) {
      val offset = index.keySpace.sharding.length + index.keySpace.sharing.length
      Some(Z2Iterator.configure(values.asInstanceOf[Z2IndexValues], offset, priority))
    } else if (index.name == S3Index.name) {
      Some(S3Iterator.configure(values.asInstanceOf[S3IndexValues], index.keySpace.sharding.length, priority))
    } else if (index.name == S2Index.name) {
      Some(S2Iterator.configure(values.asInstanceOf[S2IndexValues], index.keySpace.sharding.length, priority))
    } else {
      None
    }
  }
}
