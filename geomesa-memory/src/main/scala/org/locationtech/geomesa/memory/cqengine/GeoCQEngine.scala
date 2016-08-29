/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine

import java.util

import com.googlecode.cqengine.query.option.DeduplicationStrategy
import com.googlecode.cqengine.query.{QueryFactory, Query}
import com.googlecode.cqengine.query.simple.All
import org.locationtech.geomesa.memory.cqengine.utils.{CQEngineQueryVisitor, CQIndexingOptions}
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

class GeoCQEngine(sft: SimpleFeatureType) {
  val cqcache = CQIndexingOptions.buildIndexedCollection(sft)

  def remove(sf: SimpleFeature) {
    cqcache.remove(sf)
  }

  def add(sf: SimpleFeature) {
    cqcache.add(sf)
  }

  def addAll(sfs: util.Collection[SimpleFeature]) {
    cqcache.addAll(sfs)
  }

  def clear() {
    cqcache.clear()
  }

  // NB: We expect that FID filters have been handled previously
  def getReaderForFilter(filter: Filter): FR =
    filter match {
      case f: IncludeFilter => include(f)
      case f                => queryCQ(f, dedup = true)
      // JNH: Consider testing filter rewrite before passing to CQEngine?
    }

  def queryCQ(f: Filter, dedup: Boolean = true): FR = {
    val visitor = new CQEngineQueryVisitor(sft)

    val query: Query[SimpleFeature] = f.accept(visitor, null) match {
      case q: Query[SimpleFeature] => q
      case _ => throw new Exception(s"Filter visitor didn't recognize filter: $f.")
    }
    // TODO: Replace println with logging.
    //println(s"Querying CQEngine with $query")
    if (dedup) {
      val dedupOpt = QueryFactory.deduplicate(DeduplicationStrategy.LOGICAL_ELIMINATION)
      val queryOptions = QueryFactory.queryOptions(dedupOpt)
      new DFR(sft, new DFI(cqcache.retrieve(query, queryOptions).iterator()))
    }
    else
      new DFR(sft, new DFI(cqcache.retrieve(query).iterator()))
  }

  def include(i: IncludeFilter) = {
    println("Running Filter.INCLUDE")
    new DFR(sft, new DFI(cqcache.retrieve(new All(classOf[SimpleFeature])).iterator()))
  }

  // This is a convenience method to query CQEngine directly.
  def getReaderForQuery(query: Query[SimpleFeature]): FR = {
    new DFR(sft, new DFI(cqcache.retrieve(query).iterator))
  }
}
