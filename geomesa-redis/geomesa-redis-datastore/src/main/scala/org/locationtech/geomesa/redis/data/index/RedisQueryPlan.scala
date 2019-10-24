/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data
package index

import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.index.PartitionParallelScan
import org.locationtech.geomesa.index.api.{BoundedByteRange, FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.redis.data.util.RedisBatchScan
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import redis.clients.jedis.Response

sealed trait RedisQueryPlan extends QueryPlan[RedisDataStore] {

  /**
    * Tables being scanned
    *
    * @return
    */
  def tables: Seq[String]

  /**
    * Ranges being scanned
    *
    * @return
    */
  def ranges: Seq[BoundedByteRange]

  /**
    * Final filter applied to results
    *
    * @return
    */
  def ecql: Option[Filter]

  override def explain(explainer: Explainer, prefix: String = ""): Unit =
    RedisQueryPlan.explain(this, explainer, prefix)

  // additional explaining, if any
  protected def explain(explainer: Explainer): Unit = {}
}

object RedisQueryPlan {

  import org.locationtech.geomesa.filter.filterToString

  def explain(plan: RedisQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getSimpleName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"ECQL: ${plan.ecql.map(filterToString).getOrElse("None")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    plan.explain(explainer)
    explainer.popLevel()
  }

  private [data] def rangeToString(range: BoundedByteRange): String = {
    // based on accumulo's byte representation
    def printable(b: Byte): String = {
      val c = 0xff & b
      if (c >= 32 && c <= 126) { c.toChar.toString } else { f"%%$c%02x;" }
    }
    s"[${range.lower.map(printable).mkString("")}::${range.upper.map(printable).mkString("")}]"
  }

  // plan that will not actually scan anything
  case class EmptyPlan(
      filter: FilterStrategy,
      resultsToFeatures: CloseableIterator[Array[Byte]] => CloseableIterator[SimpleFeature]
    ) extends RedisQueryPlan {
    override val tables: Seq[String] = Seq.empty
    override val ranges: Seq[BoundedByteRange] = Seq.empty
    override val ecql: Option[Filter] = None
    override def scan(ds: RedisDataStore): CloseableIterator[SimpleFeature] =
      resultsToFeatures(CloseableIterator.empty)
  }

  // uses zrangebylex
  case class ZLexPlan(
      filter: FilterStrategy,
      tables: Seq[String],
      ranges: Seq[BoundedByteRange],
      pipeline: Boolean,
      ecql: Option[Filter], // note: will already be applied in resultsToFeatures
      resultsToFeatures: CloseableIterator[Array[Byte]] => CloseableIterator[SimpleFeature]
    ) extends RedisQueryPlan {

    import scala.collection.JavaConverters._

    override def scan(ds: RedisDataStore): CloseableIterator[SimpleFeature] = {
      val iter = tables.iterator.map(_.getBytes(StandardCharsets.UTF_8))
      val scans = iter.map(singleTableScan(ds, _))
      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        scans.foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(scans).flatMap(s => s)
      }
    }

    override protected def explain(explainer: Explainer): Unit =
      explainer(s"Pipelining: ${if (pipeline) { "enabled" } else { "disabled" }}")

    private def singleTableScan(ds: RedisDataStore, table: Array[Byte]): CloseableIterator[SimpleFeature] = {
      if (pipeline) {
        val result = Seq.newBuilder[Response[java.util.Set[Array[Byte]]]]
        result.sizeHint(ranges.length)
        WithClose(ds.connection.getResource) { jedis =>
          WithClose(jedis.pipelined()) { pipe =>
            // note: use a foreach here to ensure the calls are all executing inside our close block
            ranges.foreach(range => result += pipe.zrangeByLex(table, range.lower, range.upper))
            pipe.sync()
          }
        }
        resultsToFeatures(result.result.iterator.flatMap(_.get.iterator().asScala))
      } else {
        val results = RedisBatchScan(ds.connection, table, ranges, ds.config.queryThreads)
        SelfClosingIterator(resultsToFeatures(results), results.close())
      }
    }
  }
}
