/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
* Portions
* Copyright 2011-2016 The Apache Software Foundation
*/

package org.locationtech.geomesa.index.index

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait IndexAdapter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C] {

  /**
    * Create an insert 'statement' (but don't execute it)
    *
    * @param row row key
    * @param feature feature to be inserted
    * @return
    */
  protected def createInsert(row: Array[Byte], feature: F): W

  /**
    * Create a delete 'statement' (but don't execute it)
    *
    * @param row row key
    * @param feature feature to be deleted
    * @return
    */
  protected def createDelete(row: Array[Byte], feature: F): W

  /**
    * Create a range for scanning, with start row included and end row excluded.
    * No start/end is indicated by an empty byte array.
    *
    * @param start start of the range, inclusive. Empty byte array indicates open-ended
    * @param end end of the range, exclusive. Empty byte array indicates open-ended
    * @return
    */
  protected def createRange(start: Array[Byte], end: Array[Byte]): R

  /**
    * Creates a range for scanning a single exact row
    *
    * @param row row to scan
    * @return
    */
  protected def createRange(row: Array[Byte]): R

  /**
    * Create a config as an intermediate step for creating the query plan
    *
    * @param sft simple feature type
    * @param ds data store
    * @param filter filter
    * @param ranges ranges being scanned
    * @param ecql secondary ecql filter to apply - some filters may have already been extracted
    *             and handled by range planning
    * @param hints query hints
    * @return
    */
  protected def scanConfig(sft: SimpleFeatureType,
                           ds: DS,
                           filter: FilterStrategy[DS, F, W],
                           ranges: Seq[R],
                           ecql: Option[Filter],
                           hints: Hints): C

  /**
    * Create a query plan
    *
    * @param sft simple feature type
    * @param ds data store
    * @param filter filter
    * @param config scan config
    * @return
    */
  protected def scanPlan(sft: SimpleFeatureType,
                         ds: DS,
                         filter: FilterStrategy[DS, F, W],
                         config: C): QueryPlan[DS, F, W]
}

object IndexAdapter {

  val ZeroByte: Byte = 0x00.toByte
  val MaxByte: Byte =  0xff.toByte

  private lazy val logger = IndexAdapterLogger.log

  // helper shim to let other classes avoid importing IndexAdapter.logger
  object IndexAdapterLogger extends LazyLogging {
    def log: Logger = logger
  }

  /**
    * Splits a range up into equal parts.
    *
    * Note: currently only handles prefix ranges, which should mainly the the ones we want to expand.
    *
    * @param start start value, inclusive
    * @param stop stop value, exclusive
    * @param splits hint for the number of parts to split into
    * @return sequence of new ranges
    */
  def splitRange(start: Array[Byte], stop: Array[Byte], splits: Int): Seq[(Array[Byte], Array[Byte])] = {
    require(splits > 0 && splits < 256, "Splits must be greater than 0 and less than 256")
    if (splits == 1) {
      Seq((start, stop))
    } else if ((start.length == 0 && stop.length == 0) || java.util.Arrays.equals(ByteArrays.rowFollowingPrefix(start), stop)) {
      val increment = 256 / splits
      val bytes = (1 until splits).map(i => start :+ ((i * increment) & MaxByte).toByte)
      val first = (start, bytes.head)
      val last = (bytes.last, stop)
      val middle = if (bytes.length == 1) {
        Seq.empty
      } else {
        bytes.sliding(2).map { case Seq(l, r) => (l, r) }
      }
      Seq(first) ++ middle :+ last
    } else {
      logger.warn(s"Not splitting range [${ByteArrays.toHex(start)},${ByteArrays.toHex(stop)}] - " +
          "may want to consider implementing further split logic")
      Seq((start, stop))
    }
  }
}
