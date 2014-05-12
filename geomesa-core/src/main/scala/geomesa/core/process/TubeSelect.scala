package geomesa.process

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.index.Constants
import geomesa.core.util.MultiCollection
import java.util.Date
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.NameImpl
import org.geotools.feature.collection.SortedSimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.visitor._
import org.geotools.filter.{SortByImpl, AttributeExpressionImpl}
import org.geotools.process.factory.{DescribeResult, DescribeParameter, DescribeProcess}
import org.geotools.process.vector.VectorProcess
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortOrder
import scala.collection.mutable.ListBuffer
import org.geotools.filter.text.ecql.ECQL
import org.apache.log4j.Logger

@DescribeProcess(
  title = "Performs a tube select on one feature collection based on another feature collection",
  description = "Returns a feature collection"
)
class TubeSelect extends VectorProcess {

  @DescribeResult(description = "Output feature collection")
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
                 name = "filter",
                 min = 0,
                 description = "The filter to apply to the featureCollection")
               filter: Filter,

               @DescribeParameter(
                 name = "maxSpeed",
                 min = 0,
                 description = "Max speed of the object in m/s for none, straightLine, aroundLand gapfill methods")
               maxSpeed: Long,

               @DescribeParameter(
                 name = "maxTime",
                 min = 0,
                 description = "Time as milliseconds for none, straightLine, aroundLand gapfill methods")
               maxTime: Long,

               @DescribeParameter(
                 name = "bufferSize",
                 min = 0,
                 description = "Buffer size in meters to use instead of maxSpeed/maxTime calculation")
               bufferSize: Double,

               @DescribeParameter(
                 name = "gapFill",
                 min = 0,
                 description = "Method of filling gap (none, straightLine, aroundLand, possiblePath)")
               gapFill: String,

               @DescribeParameter(
                 name = "percentIncrease",
                 min = 0,
                 description = "Percent Increase of radius of buffer between points for possiblePath gapfill")
               percentIncrease: Int

               ): SimpleFeatureCollection = {

    // assume for now that firstFeatures is a singleton collection
    val tubeVisitor = new TubeVisitor(
                                      tubeFeatures,
                                      featureCollection,
                                      Option(filter).getOrElse(Filter.INCLUDE),
                                      Option(maxSpeed).getOrElse(0),
                                      Option(maxTime).getOrElse(0),
                                      Option(bufferSize).getOrElse(0),
                                      Option(gapFill).getOrElse(GapFill.NONE),
                                      Option(percentIncrease).getOrElse(0))
    featureCollection.accepts(tubeVisitor, new NullProgressListener)
    tubeVisitor.getResult.asInstanceOf[TubeResult].results
  }

}

object GapFill {
  val NONE = "none"
  val STRAIGHT_LINE = "straightLine"
  val AROUND_LAND = "aroundLand"
  val POSSIBLE_PATH = "possiblePath"
}

class TubeVisitor(
                   val tubeFeatures: SimpleFeatureCollection,
                   val featureCollection: SimpleFeatureCollection,
                   val filter: Filter,
                   val maxSpeed: Long,
                   val maxTime: Long,
                   val bufferSize: Double,
                   val gapFill: String,
                   val percentIncrease: Int
                   ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[TubeVisitor])

  var resultCalc: TubeResult = null

  def visit(feature: Feature): Unit = {}

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) {
    resultCalc = TubeResult(r)
  }

  val ff  = CommonFactoryFinder.getFilterFactory2

  // Needs to be optimized to do fewer queries...currently does one query
  // per feature in the source since we have to buffer each feature by
  // time and geometry.
  def tubeSelect(source: SimpleFeatureSource, query: Query): SimpleFeatureCollection = {
    log.debug("Tubing on with query: "+query)
    // Sort and buffer geometry - sorting needs to be done for line filling methods
    val sortedTube = TubeVisitor.sortByDate(tubeFeatures)
    val buffered = TubeVisitor.bufferPoints(sortedTube, bufferDistance)

    val geomProperty = ff.property(source.getSchema.getGeometryDescriptor.getName)

    val itr = buffered.features
    val collections = new ListBuffer[SimpleFeatureCollection]
    while(itr.hasNext) {
      val sf = itr.next
      val geom = sf.getDefaultGeometry
      val geomFilter = ff.within(geomProperty, ff.literal(geom))

      val minDate = new Date(sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date].getTime - maxTime)
      val maxDate = new Date(sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date].getTime + maxTime)
      val dateProperty = ff.property(tubeFeatures.getSchema.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String])
      val dtgFilter = ff.between(dateProperty, ff.literal(minDate), ff.literal(maxDate))

      val combinedFilter = ff.and(List(query.getFilter, geomFilter, dtgFilter, filter))
      log.debug("Running tube subquery with filter: "+ ECQL.toCQL(combinedFilter))
      collections += source.getFeatures(combinedFilter)
    }
    new MultiCollection(source.getSchema, collections)
  }

  def bufferDistance: Double = if(bufferSize > 0) bufferSize else maxSpeed * maxTime*1000

}

object TubeVisitor {

  val calc = new GeodeticCalculator()
  val geoFac = new GeometryFactory

  // calculate degrees from meters at a given point
  def metersToDegrees(meters: Double, point: Point) = {
    calc.setStartingGeographicPoint(point.getX, point.getY)
    calc.setDirection(0, meters)
    val dest2D = calc.getDestinationGeographicPoint
    val destPoint = geoFac.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
    point.distance(destPoint)
  }

  def bufferPoint(point: Point, meters: Double) = point.buffer(metersToDegrees(meters, point))

  def bufferPoints(features: SimpleFeatureCollection, meters: Double) = {
    val itr = features.features()
    val buffered = new ListBuffer[SimpleFeature]
    val builder = new SimpleFeatureBuilder(features.getSchema)

    while(itr.hasNext){
      val sf = itr.next()
      val geom = sf.getDefaultGeometry.asInstanceOf[Point]
      val bufPoint = bufferPoint(geom, meters)

      builder.init(sf)
      val newSf = builder.buildFeature(sf.getID)
      newSf.setDefaultGeometry(bufPoint)
      buffered += newSf
    }

    new ListFeatureCollection(features.getSchema, buffered)
  }

  def sortByDate(features: SimpleFeatureCollection) = {
    val dateField = new AttributeExpressionImpl(new NameImpl(Constants.SF_PROPERTY_START_TIME))
    val sortBy = new SortByImpl(dateField, SortOrder.DESCENDING)
    new SortedSimpleFeatureCollection(features, Array(sortBy))
  }

}

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult