package geomesa.core.process.rank

import java.util

import com.vividsolutions.jts.geom.{Geometry, LineString}
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.core.index
import geomesa.core.process.query.QueryProcess
import geomesa.utils.geotools.Conversions._
import org.apache.log4j.Logger
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/18/14
 * Time: 5:37 PM
 */
@DescribeProcess(
title = "Rank Features", // "Geomesa-enabled Ranking of Feature Groups in Proximity to Route",
description = "Performs a proximity search on a Geomesa feature collection using another feature collection as input." /* +
  " Then groups the features according to a key and computes ranking metrics thats measures the prominence of " +
  "each key within the search region. The computed metrics measure the frequency of each feature group within the " +
  "search region, relative frequency in the surrounding area, the spatial diversity of the feature within the " +
  "region, and evidence of motion through the search region." */
)
class RouteRankProcess {

  private val log = Logger.getLogger(classOf[RouteRankProcess])

  //@DescribeResult(description = "Ranking metrics for each key value")
  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "This must be a single line string for now")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "The data set to query for matching features")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "bufferDistance",
                 description = "Buffer size in meters")
               bufferDistance: java.lang.Double,

               @DescribeParameter(
                 name = "keyField",
                 description = "The name of the key attribute to group by")
               keyField: String,

               @DescribeParameter(
                  name = "skip",
                  min = 0,
                  defaultValue = "0",
                  description = "The number of results to skip (for paging)")
                skip: Int,

               @DescribeParameter(
                 name = "max",
                 min = 0,
                 defaultValue = RankingDefaults.defaultMaxResultsStr,
                 description = "The maximum number of results to return")
               max: Int,

               @DescribeParameter(
                 name = "sortBy",
                 min = 0,
                 defaultValue = RankingDefaults.defaultResultsSortField,
                 description = "The field to sort by")
               sortBy: String

               ): ResultBean = {

    log.info("Attempting Geomesa Route Rank on collection type " + dataFeatures.getClass.getName)

    if (!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support geomesa proximity search: " + dataFeatures.getClass.getName)
    }
    if (dataFeatures.isInstanceOf[ReTypingFeatureCollection]) {
      log.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val route = extractRoute(inputFeatures)
    val rv = route match {
      case Some(r) =>
        def getTimeAttrName(sfc: SimpleFeatureCollection) =
          index.getDtgDescriptor(sfc.getSchema).map{_.getLocalName}.getOrElse("geomesa_index_start_time")
        val spec = new SfSpec(keyField, getTimeAttrName(dataFeatures))
        val routeShape = r.route.bufferMeters(bufferDistance)
        val boxShape = boundingSquare(routeShape)
        val ff = CommonFactoryFinder.getFilterFactory2
        val boxFilter = ff.intersects(ff.property(inputFeatures.getSchema.getGeometryDescriptor.getLocalName),
          ff.literal(boxShape))
        val routeFilter = ff.intersects(ff.property(dataFeatures.getSchema.getGeometryDescriptor.getLocalName),
          ff.literal(routeShape))
        val qp = new QueryProcess
        val routeSearchResults = qp.execute(dataFeatures, routeFilter)
        val boxSearchResults = qp.execute(dataFeatures, boxFilter)
        val routeFeatures = new SimpleFeatureWithDateTimeAndKeyCollection(routeSearchResults, spec)
        val boxFeatures = new SimpleFeatureWithDateTimeAndKeyCollection(boxSearchResults, spec)
        val routeAndFeatures = new RouteAndSurroundingFeatures(r, boxFeatures, routeFeatures)
        routeAndFeatures.rank(boxShape.getEnvelopeInternal, List(routeShape))
      case _ =>
        log.warn("WARNING: input feature to rank process must be a single LineString")
        Map[String, RankingValues]()
    }
    ResultBean.fromRankingValues(rv, sortBy, skip, max)
  }

  private def extractRoute(inputFeatures: SimpleFeatureCollection): Option[Route] = {
    if (inputFeatures.size() == 1) {
      val routeTry = for {
        ls <- Try(inputFeatures.features().take(1).next().getDefaultGeometry.asInstanceOf[LineString])
        ls4326 <- Try(if (ls.getSRID == 4326) ls else {
          val sourceCRS = inputFeatures.getSchema.getCoordinateReferenceSystem
          val transform = CRS.findMathTransform(sourceCRS, DefaultGeographicCRS.WGS84, true)
          JTS.transform(ls, transform).asInstanceOf[LineString]
        })
        route = new Route(ls4326)
      } yield route
      routeTry.toOption
    }
    else None
  }

  private def boundingSquare(bufferedRouteGeometry: Geometry) = {
    val env1 = bufferedRouteGeometry.getEnvelopeInternal
    val diffLat = env1.getMaxY - env1.getMinY
    val diffLon = env1.getMaxX - env1.getMinX
    val centerLat = (env1.getMaxY + env1.getMinY) / 2.0
    val centerLon = (env1.getMaxX + env1.getMinX) / 2.0
    val delta = (if (diffLat > diffLon) diffLat else diffLon) / 2.0
    val env2 = new ReferencedEnvelope(centerLon - delta, centerLon + delta, centerLat - delta, centerLat + delta,
      DefaultGeographicCRS.WGS84)
    JTS.toGeometry(env2)
  }
}

case class Counts(@BeanProperty route: Int, @BeanProperty box: Int)
case class CellsCovered(@BeanProperty box: Int, @BeanProperty route: Int,
                        @BeanProperty percentageOfRouteCovered: Double, @BeanProperty avgPerRouteCell: Double)
case class RouteCellDeviation(@BeanProperty stddev: Double, @BeanProperty scaledStddev: Double,
                              @BeanProperty deviationScore: Double)
case class TfIdf(@BeanProperty idf: Double, @BeanProperty tfIdf: Double, @BeanProperty scaledTfIdf: Double)
case class Combined(@BeanProperty scoreNoMotion: Double, @BeanProperty score: Double)
case class RankingValuesBean(@BeanProperty key: String, @BeanProperty counts: Counts,
                             @BeanProperty cellsCovered: CellsCovered,
                             @BeanProperty routeCellDeviation: RouteCellDeviation, @BeanProperty tfIdf: TfIdf,
                             @BeanProperty motionEvidence: EvidenceOfMotion, @BeanProperty combined: Combined) {
  def toRankingValues(gridSize: Int, nRouteGridCells: Int): RankingValues = {
    RankingValues(counts.route, counts.box, cellsCovered.box, cellsCovered.route, routeCellDeviation.stddev,
      motionEvidence, gridSize, nRouteGridCells)
  }
}

case class ResultBean(@BeanProperty results: java.util.List[RankingValuesBean], @BeanProperty maxScore: Double,
                      @BeanProperty gridSize: Int, @BeanProperty nRouteGridCells: Int, @BeanProperty sortBy: String) {
  def keyMap = results.asScala.groupBy(_.key)

  def merge(other: ResultBean): ResultBean = {
    val km1 = keyMap
    val km2 = other.keyMap
    val allKeys = km1.keySet ++ km2.keySet
    def emptyBuffer = mutable.Buffer[RankingValuesBean]()
    val valueLists = allKeys.map(k => (k, km1.getOrElse(k, emptyBuffer) ++ km2.getOrElse(k, emptyBuffer)))
    ResultBean.fromRankingValues(valueLists.map {
      case (key, resultList) =>
        (key, resultList.foldLeft(RankingValues.emptyOne(gridSize, nRouteGridCells)) {
          case (combined, current) => combined.merge(current.toRankingValues(gridSize, nRouteGridCells))
        })
    }.toMap, sortBy)
  }
}

object ResultBean {
  def fromRankingValues(rankingValues: Map[String,RankingValues], sortBy: String, skip: Int = 0, max: Int = -1) = {
    val (nTubeCells, gridSize) = rankingValues.headOption.
      flatMap(h => Some((h._2.nTubeCells, h._2.gridDivisions))).getOrElse((0,0))
    ResultBean(new util.ArrayList(rankingValues.map {
      case (key, rv) => RankingValuesBean(key, Counts(rv.tubeCount, rv.boxCount),
        CellsCovered(rv.boxCellsCovered, rv.tubeCellsCovered, rv.percentageOfTubeCellsCovered, rv.avgPerTubeCell),
        RouteCellDeviation(rv.tubeCellsStddev, rv.scaledTubeCellStddev, rv.tubeCellDeviationScore),
        TfIdf(rv.idf, rv.tfIdf, rv.scaledTfIdf),
        EvidenceOfMotion(rv.motionEvidence.total, rv.motionEvidence.max, rv.motionEvidence.stddev),
        Combined(rv.combinedScoreNoMotion, rv.combinedScore))
    }.toList.sortBy(_.combined.score * -1.0).
      slice(skip, if (max > 0) skip + max else Int.MaxValue).asJava),
      rankingValues.maxBy(_._2.combinedScore)._2.combinedScore, gridSize, nTubeCells, sortBy)
  }
}

