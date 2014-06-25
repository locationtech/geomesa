package geomesa.core.process.rank

import java.util.concurrent.atomic.AtomicInteger

import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.geom.{Geometry, LineString}
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.core.index
import geomesa.core.process.query.QueryProcess
import geomesa.core.process.tube.{TubeBuilder, TubeSelectProcessInputs, TubeSelectProcess}
import geomesa.utils.text.WKTUtils
import org.apache.log4j.Logger
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.{ReferencedEnvelope, JTS}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.filter.Filter

import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/23/14
 * Time: 3:25 PM
 */
@DescribeProcess(
  title = "Rank Features along Track", // "Geomesa-enabled Ranking of Feature Groups in Proximity to Track",
  description = "Performs a proximity search on a Geomesa feature collection using another feature collection as " +
    "input. Then groups the features according to a key and computes ranking metrics thats measures the prominence of" +
    " each key within the search region. The computed metrics measure the frequency of each feature group within the " +
    "search region, relative frequency in the surrounding area, the spatial diversity of the feature within the " +
    "region, and evidence of motion through the search region. Unlike RouteRank, incorporates the time of the track."
)
class TrackRankProcess {

  private val log = Logger.getLogger(classOf[RouteRankProcess])

  //@DescribeResult(description = "Ranking metrics for each key value")
  @DescribeResult(description = "Output ranking scores")
  def execute(
               @DescribeParameter(
                 name = "tubeFeatures",
                 description = "Input feature collection (must have geometry and datetime)")
               tubeFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "featureCollection",
                 description = "The data set to query for matching features")
               featureCollection: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "keyField",
                 description = "The name of the key attribute to group by")
               keyField: String,

               @DescribeParameter(
                 name = "filter",
                 min = 0,
                 description = "The filter to apply to the featureCollection")
               filter: Filter,

               @DescribeParameter(
                 name = "maxSpeed",
                 min = 0,
                 description = "Max speed of the object in m/s for nofill & line gapfill methods")
               maxSpeed: java.lang.Long,

               @DescribeParameter(
                 name = "maxTime",
                 min = 0,
                 description = "Time as seconds for nofill & line gapfill methods")
               maxTime: java.lang.Long,

               @DescribeParameter(
                 name = "bufferSize",
                 min = 0,
                 description = "Buffer size in meters to use instead of maxSpeed/maxTime calculation")
               bufferSize: java.lang.Double,

               @DescribeParameter(
                 name = "maxBins",
                 min = 0,
                 description = "Number of bins to use for breaking up query into individual queries")
               maxBins: java.lang.Integer,

               @DescribeParameter(
                 name = "gapFill",
                 min = 0,
                 description = "Method of filling gap (nofill, line)")
               gapFill: String,

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
    new TrackFeatureGroupRanker(TubeSelectProcessInputs(tubeFeatures, featureCollection, filter, maxSpeed, maxTime,
      bufferSize, maxBins, gapFill), keyField, skip, max, sortBy).groupAndRank
  }
}

class TrackFeatureGroupRanker(tubeSelectInputs: TubeSelectProcessInputs,
                              override val keyField: String,
                              override val skip: Int,
                              override val max: Int,
                              override val sortBy: String) extends FeatureGroupRanker {
  val tubeSelectParameters = tubeSelectInputs.toParameters
  override def queryRoute(route: Route) = {
    val ts = new TubeSelectProcess
    ts.execute(tubeSelectInputs.tubeFeatures, tubeSelectInputs.featureCollection, tubeSelectInputs.filter,
      tubeSelectInputs.maxSpeed, tubeSelectInputs.maxTime, tubeSelectInputs.bufferSize, tubeSelectInputs.maxBins,
      tubeSelectInputs.gapFill)
  }

  override def dataFeatures = tubeSelectInputs.featureCollection

  override def bufferMeters = tubeSelectParameters.bufferDistance

  override def extractRoute = {
    Try(new Route(new LineStringTubeBuilder(tubeSelectParameters.tubeFeatures, tubeSelectParameters.bufferDistance,
      tubeSelectParameters.maxBins).createTube.next().getDefaultGeometry.asInstanceOf[LineString])).toOption
  }
}

class LineStringTubeBuilder(tubeFeatures: SimpleFeatureCollection, bufferDistance: Double, maxBins: Int)
  extends TubeBuilder(tubeFeatures, bufferDistance, maxBins) {

  override def createTube = {
    val transformed = transform(tubeFeatures, dtgField)
    val sortedTube = transformed.toSeq.sortBy { sf => getStartTime(sf).getTime }
    val coords = sortedTube.map { getGeom(_).getCentroid.getCoordinate }
    val cas = new CoordinateArraySequence(coords.toArray)
    val times = sortedTube.map(getStartTime(_))
    val t1 = times.min
    val t2 = times.max
    val ls = new LineString(cas, geoFac)
    builder.reset
    Iterator(builder.buildFeature("0", Array(ls, t1, t2)))
  }
}