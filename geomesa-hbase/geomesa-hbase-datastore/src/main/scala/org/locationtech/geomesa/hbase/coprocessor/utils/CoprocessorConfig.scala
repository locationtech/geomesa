/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.utils

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.locationtech.geomesa.hbase.coprocessor.{FILTER_OPT, SCAN_OPT}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

case class CoprocessorConfig(options: Map[String, String],
                             bytesToFeatures: Array[Byte] => SimpleFeature,
                             reduce: CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = (i) => i) {
  def configureScanAndFilter(scan: Scan, filterList: FilterList): Map[String, String] = {
    import org.apache.hadoop.hbase.util.Base64
    options.updated(FILTER_OPT, Base64.encodeBytes(filterList.toByteArray))
      .updated(SCAN_OPT, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
  }
}
