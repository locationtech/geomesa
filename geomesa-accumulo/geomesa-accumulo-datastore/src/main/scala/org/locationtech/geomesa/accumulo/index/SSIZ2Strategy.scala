package org.locationtech.geomesa.accumulo.index

import java.nio.charset.StandardCharsets

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.tables.{SSIZ2SFC, GeoMesaTable, Z2Table, SSIZ2Table}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.FilterHelper._
import org.apache.accumulo.core.data.{Range => aRange}
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.filter.Filter

class SSIZ2Strategy(val filter: QueryFilter)  extends Strategy with LazyLogging with IndexFilterHelpers {
  /**
    * Plans the query - strategy implementations need to define this
    */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType): QueryPlan = {

    import QueryHints.{LOOSE_BBOX, RichHints}
    import Z2IdxStrategy._
    import org.locationtech.geomesa.filter.FilterHelper._
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    val ds  = queryPlanner.ds
    val sft = queryPlanner.sft

    val isInclude = QueryFilterSplitter.isFullTableScan(filter)

    if (isInclude) {
      // allow for full table scans - we use the z2 index for queries that can't be satisfied elsewhere
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    // TODO GEOMESA-1215 this can handle OR'd geoms, but the query splitter won't currently send them
    val geometryToCover =
      filter.singlePrimary.flatMap(extractSingleGeometry(_, sft.getGeomField, sft.isPoints)).getOrElse(WholeWorldPolygon)

    output(s"GeomsToCover: $geometryToCover")

    val looseBBox = if (hints.containsKey(LOOSE_BBOX)) Boolean.unbox(hints.get(LOOSE_BBOX)) else ds.config.looseBBox

    val ecql: Option[Filter] = if (isInclude || !looseBBox || sft.nonPoints) {
      // if this is a full table scan, we can just use the filter option to get the secondary ecql
      // if the user has requested strict bounding boxes, we apply the full filter
      // if this is a non-point geometry, the index is coarse-grained, so we apply the full filter
      filter.filter
    } else {
      // for normal bboxes, the index is fine enough that we don't need to apply the filter on top of it
      // this may cause some minor errors at extremely fine resolution, but the performance is worth it
      // if we have a complicated geometry predicate, we need to pass it through to be evaluated
      val complexGeomFilter = filterListAsAnd(filter.primary.filter(isComplicatedSpatialFilter))
      (complexGeomFilter, filter.secondary) match {
        case (Some(gf), Some(fs)) => filterListAsAnd(Seq(gf, fs))
        case (None, fs)           => fs
        case (gf, None)           => gf
      }
    }

    val (iterators, kvsToFeatures, colFamily, hasDupes) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          ???
          // TODO GEOMESA-1254 per-attribute vis + bins
//          val idOffset = SSIZ2Table.getIdRowOffset(sft)
//          (Seq(BinAggregatingIterator.configurePrecomputed(sft, ecql, hints, idOffset, sft.nonPoints)), SSIZ2Table.BIN_CF)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, ecql, hints, sft.nonPoints)
          (Seq(iter), SSIZ2Table.FULL_CF)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf, false)
    } else if (hints.isDensityQuery) {
      val iter = Z2DensityIterator.configure(sft, ecql, hints)
      ??? //(Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), SSIZ2Table.FULL_CF, false)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, ecql, hints, sft.nonPoints)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), SSIZ2Table.FULL_CF, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, ecql, hints, sft.nonPoints)
      (Seq(iter), queryPlanner.defaultKVsToFeatures(hints), SSIZ2Table.FULL_CF, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, ecql, hints).toSeq
      (iters, queryPlanner.defaultKVsToFeatures(hints), SSIZ2Table.FULL_CF, false)
    }

    val ssiz2Table = ds.getTableName(sft.getTypeName, SSIZ2Table)
    val numThreads = ds.getSuggestedThreads(sft.getTypeName, SSIZ2Table)

    val (ranges: Seq[org.apache.accumulo.core.data.Range], z2Iter: Option[IteratorSetting]) = if (isInclude) {
      val range = if (sft.isTableSharing) {
        aRange.prefix(new Text(sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)))
      } else {
        new aRange()
      }
      (Seq(range), None)
    } else {
      val env = geometryToCover.getEnvelopeInternal
      val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

      val prefixes = if (sft.isTableSharing) {
        val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
        SSIZ2Table.SPLIT_ARRAYS.map(ts ++ _)
      } else {
        SSIZ2Table.SPLIT_ARRAYS
      }

      val ranges = getRanges(prefixes, (lx, ux), (ly, uy))

      (ranges, None)
    }

    val perAttributeIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft, SSIZ2Table))
    }
    val cf = if (perAttributeIter.isEmpty) colFamily else GeoMesaTable.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators ++ z2Iter
    BatchScanPlan(filter, ssiz2Table, ranges, iters, Seq(cf), kvsToFeatures, numThreads, hasDupes)
  }

  def getRanges(prefixes: Seq[Array[Byte]], x: (Double, Double), y: (Double, Double)): Seq[aRange] = {
    SSIZ2SFC.tieredranges(x, y).flatMap { case (tierBytes, indexRange) =>
      val startBytes = Longs.toByteArray(indexRange.lower)
      val endBytes = Longs.toByteArray(indexRange.upper)
      prefixes.map { prefix =>
        val start = new Text(Bytes.concat(prefix, tierBytes, startBytes))
        val end = aRange.followingPrefix(new Text(Bytes.concat(prefix, tierBytes, endBytes)))
        new aRange(start, true, end, false)
      }
    }
  }
}
