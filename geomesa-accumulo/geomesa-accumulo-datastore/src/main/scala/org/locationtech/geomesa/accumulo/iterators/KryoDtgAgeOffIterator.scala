/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators


import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.joda.time.format.{ISOPeriodFormat, PeriodFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.iterators.KryoDtgAgeOffIterator._
import org.opengis.feature.simple.SimpleFeature

/**
  * Age off data based on the dtg value stored in the SimpleFeature
  */
class KryoDtgAgeOffIterator extends KryoLazyAgeOffFilter {

  private var dtgIdx: Int = -1
  private var minTs: Long = -1

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val copy = super[KryoLazyAgeOffFilter].deepCopy(env).asInstanceOf[KryoDtgAgeOffIterator]

    copy.dtgIdx = dtgIdx
    copy.minTs = minTs

    copy
  }

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    super[KryoLazyAgeOffFilter].init(source, options, env)

    val now = DateTime.now(DateTimeZone.UTC)
    val retention = options.get(Options.RetentionPeriod)
    minTs = minimumTimestamp(now, retention)
    dtgIdx = IteratorCache.dtgIndex(spec, sft)
  }

  override def accept(sf: SimpleFeature): Boolean = {
    val ts = reusableSF.getDateAsLong(dtgIdx)
    ts > minTs
  }
}

object KryoDtgAgeOffIterator {

  object Options {
    val RetentionPeriod = "retention"
  }

  val periodFormat: PeriodFormatter = ISOPeriodFormat.standard()

  def minimumTimestamp(now: DateTime, pStr: String): Long = {
    val p = periodFormat.parsePeriod(pStr)
    now.minus(p).getMillis
  }

}