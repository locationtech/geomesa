package geomesa.core.process.rank

import com.vividsolutions.jts.geom.{Geometry, LineString}
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.core.index
import geomesa.core.process.query.QueryProcess
import geomesa.core.process.tube.TubeSelectProcess
import org.apache.log4j.Logger
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.{ReferencedEnvelope, JTS}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.filter.Filter
import geomesa.utils.geotools.Conversions._


import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/23/14
 * Time: 3:37 PM
 */
trait FeatureGroupRanker {

  private val log = Logger.getLogger(classOf[RouteRankProcess])

  def dataFeatures: SimpleFeatureCollection
  def extractRoute: Option[Route]
  def keyField: String
  def bufferMeters: Double
  def queryRoute(route: Route): SimpleFeatureCollection
  def skip: Int
  def max: Int
  def sortBy: String

  def groupAndRank: ResultBean = {

    log.info("Attempting Geomesa Route Rank on collection type " + dataFeatures.getClass.getName)
    if (!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support geomesa proximity search: "
        + dataFeatures.getClass.getName)
    }
    if (dataFeatures.isInstanceOf[ReTypingFeatureCollection]) {
      log.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val route = extractRoute
    val rv = route match {
      case Some(r) =>
        def getTimeAttrName(sfc: SimpleFeatureCollection) =
          index.getDtgDescriptor(sfc.getSchema).map{_.getLocalName}.getOrElse("geomesa_index_start_time")
        val spec = new SfSpec(keyField, getTimeAttrName(dataFeatures))
        val routeShape = r.route.bufferMeters(bufferMeters)
        val boxShape = boundingSquare(routeShape)
        val ff = CommonFactoryFinder.getFilterFactory2
        val boxFilter = ff.intersects(ff.property(dataFeatures.getSchema.getGeometryDescriptor.getLocalName),
          ff.literal(boxShape))
        val qp = new QueryProcess
        val routeSearchResults = queryRoute(r)
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