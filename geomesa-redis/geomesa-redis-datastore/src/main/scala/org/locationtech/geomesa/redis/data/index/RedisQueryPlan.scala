/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.redis.data
package index

import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, QueryStrategyPlan, ResultsToFeatures}
import org.locationtech.geomesa.index.api.{BoundedByteRange, QueryStrategy}
import org.locationtech.geomesa.index.planning.LocalQueryRunner.LocalProcessor
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.redis.data.index.RedisIndexAdapter.RedisResultsToFeatures
import org.locationtech.geomesa.redis.data.util.RedisBatchScan
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import redis.clients.jedis.{Jedis, Response, UnifiedJedis}

import java.nio.charset.StandardCharsets

sealed trait RedisQueryPlan extends QueryStrategyPlan {

  override type Results = SimpleFeature

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

  override def explain(explainer: Explainer): Unit = RedisQueryPlan.explain(this, explainer)

  protected def moreExplaining(explainer: Explainer): Unit = {}
}

object RedisQueryPlan {

  def explain(plan: RedisQueryPlan, explainer: Explainer): Unit = {
    explainer.pushLevel(s"Plan: ${plan.getClass.getSimpleName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    plan.moreExplaining(explainer)
    explainer(s"Reduce: ${plan.reducer.getOrElse("none")}")
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
  case class EmptyPlan(strategy: QueryStrategy, reducer: Option[FeatureReducer] = None) extends RedisQueryPlan {
    override val tables: Seq[String] = Seq.empty
    override val ranges: Seq[BoundedByteRange] = Seq.empty
    override val resultsToFeatures: ResultsToFeatures[SimpleFeature] = ResultsToFeatures.empty
    override val sort: Option[Seq[(String, Boolean)]] = None
    override val maxFeatures: Option[Int] = None
    override val projection: Option[QueryReferenceSystems] = None
    override def scan(): CloseableIterator[SimpleFeature] = CloseableIterator.empty
  }

  // uses zrangebylex
  case class ZLexPlan(
      ds: RedisDataStore,
      strategy: QueryStrategy,
      tables: Seq[String],
      ranges: Seq[BoundedByteRange],
      pipeline: Boolean,
      processor: LocalProcessor,
      resultsToFeatures: ResultsToFeatures[SimpleFeature],
      projection: Option[QueryReferenceSystems]
    ) extends RedisQueryPlan {

    import scala.collection.JavaConverters._

    override def reducer: Option[FeatureReducer] = processor.reducer
    // handled in the local processor
    override def sort: Option[Seq[(String, Boolean)]] = None
    override def maxFeatures: Option[Int] = None

    override def scan(): CloseableIterator[SimpleFeature] = {
      // query guard hook - also handles full table scan checks
      strategy.runGuards(ds)
      val toFeatures = new RedisResultsToFeatures(strategy.index, strategy.index.sft)
      val iter = tables.iterator.map(_.getBytes(StandardCharsets.UTF_8))
      val scans = iter.map(singleTableScan(ds, _))
      if (ds.config.queries.parallelPartitionScans) {
        // kick off all the scans at once
        processor(scans.foldLeft(CloseableIterator.empty[Array[Byte]])(_ concat _).map(toFeatures.apply))
      } else {
        // kick off the scans sequentially as they finish
        processor(SelfClosingIterator(scans).flatMap(s => s.map(toFeatures.apply)))
      }
    }

    override def moreExplaining(explainer: Explainer): Unit = {
      import org.locationtech.geomesa.index.conf.QueryHints.RichHints
      explainer(s"Pipelining: ${if (pipeline) { "enabled" } else { "disabled" }}")
      // filter, transforms, sort, max features are all captured in the local processor so pull them out of the hints instead of the plan
      explainer(s"ECQL: ${processor.filter.fold("none")(FilterHelper.toString)}")
      explainer(s"Transform: ${strategy.hints.getTransform.fold("none")(t => s"${t._1} ${SimpleFeatureTypes.encodeType(t._2)}")}")
      explainer(s"Sort: ${strategy.hints.getSortFields.fold("none")(_.mkString(", "))}")
      explainer(s"Max Features: ${strategy.hints.getMaxFeatures.getOrElse("none")}")
    }

    private def singleTableScan(ds: RedisDataStore, table: Array[Byte]): CloseableIterator[Array[Byte]] = {
      if (pipeline) {
        val result = Seq.newBuilder[Response[java.util.List[Array[Byte]]]]
        result.sizeHint(ranges.length)
        WithClose(ds.connection.getResource) {
          case jedis: Jedis =>
            WithClose(jedis.pipelined()) { pipe =>
              // note: use a foreach here to ensure the calls are all executing inside our close block
              ranges.foreach(range => result += pipe.zrangeByLex(table, range.lower, range.upper))
              pipe.sync()
            }
          case jedis: UnifiedJedis =>
            WithClose(jedis.pipelined()) { pipe =>
              // note: use a foreach here to ensure the calls are all executing inside our close block
              ranges.foreach(range => result += pipe.zrangeByLex(table, range.lower, range.upper))
              pipe.sync()
            }
        }
        result.result.iterator.flatMap(_.get.iterator().asScala)
      } else {
        RedisBatchScan(ds.connection, table, ranges, ds.config.queries.threads)
      }
    }
  }
}
