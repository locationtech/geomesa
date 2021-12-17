/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.conf.{FilterCompatibility, QueryHints}
import org.locationtech.geomesa.index.conf.FilterCompatibility.FilterCompatibility
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.z3.Z3IndexValues

class Z3Iterator extends RowFilterIterator[Z3Filter](Z3Filter)

object Z3Iterator {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  @deprecated
  def configure(
      values: Z3IndexValues,
      offset: Int,
      compatibility: Option[FilterCompatibility],
      priority: Int): IteratorSetting = {
    val hints = new Hints()
    compatibility.foreach(c => hints.put(QueryHints.FILTER_COMPAT, c.toString))
    configure(values, offset, hints, priority)
  }

  /**
   * Configure the iterator
   *
   * @param values index values
   * @param offset offset for z-value in each row
   * @param hints query hints
   * @param priority iterator priority
   * @return
   */
  def configure(
      values: Z3IndexValues,
      offset: Int,
      hints: Hints,
      priority: Int): IteratorSetting = {

    val opts = hints.getFilterCompatibility match {
      case None =>
        Z3Filter.serializeToStrings(Z3Filter(values, hints)) + (RowFilterIterator.RowOffsetKey -> offset.toString)

      case Some(FilterCompatibility.`1.3`) =>
        val Z3IndexValues(sfc, _, bounds, _, times, _) = values
        val xyOpts = bounds.map { case (xmin, ymin, xmax, ymax) =>
          s"${sfc.lon.normalize(xmin)}:${sfc.lat.normalize(ymin)}:" +
              s"${sfc.lon.normalize(xmax)}:${sfc.lat.normalize(ymax)}"
        }
        val tOpts = times.toSeq.sortBy(_._1).map { case (bin, times) =>
          val time = times.map { case (t1, t2) =>
            s"${sfc.time.normalize(t1)}:${sfc.time.normalize(t2)}"
          }
          s"$bin;${time.mkString(";")}"
        }
        Map(
          "zxy" -> xyOpts.mkString(";"),
          "zt"  -> tOpts.mkString(","),
          "zo"  -> offset.toString,
          "zl"  -> "8"
        )

      case Some(c) =>
        throw new NotImplementedError(s"Unknown compatibility flag: '$c'")
    }

    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])
    opts.foreach { case (k, v) => is.addOption(k, v) }
    is
  }
}