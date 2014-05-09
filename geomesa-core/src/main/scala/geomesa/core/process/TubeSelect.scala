package geomesa.process

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.index.Constants
import geomesa.core.util.CompositeFeatureCollection
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
import org.geotools.process.vector.{CollectGeometries, VectorProcess}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortOrder
import scala.collection.mutable.ListBuffer

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
                 name = "gapFill",
                 description = "Method of filling gap (none, straightLine, aroundLand, possiblePath)")
               gapFill: String,

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
                 name = "percentIncrease",
                 min = 0,
                 description = "Percent Increase of radius of buffer between points for possiblePath gapfill")
               percentIncrease: Int

               ): SimpleFeatureCollection = {

    // assume for now that firstFeatures is a singleton collection
    val tubeVisitor = new TubeVisitor(tubeFeatures, featureCollection, Option(filter).getOrElse(Filter.INCLUDE), gapFill, maxSpeed, maxTime, percentIncrease)
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
                   val gapFill: String,
                   val maxSpeed: Long,
                   val maxTime: Long,
                   val percentIncrease: Int
                   ) extends FeatureCalc {

  var resultCalc: TubeResult = null

  def visit(feature: Feature): Unit = {}

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) {
    resultCalc = TubeResult(r)
  }

  def tubeSelect(source: SimpleFeatureSource, query: Query): SimpleFeatureCollection = {
//    val dtgFilter = getDtgFilter
    val tubeGeometries = getTubeGeometries

    val geomProperty = ff.property(source.getSchema.getGeometryDescriptor.getName)

    val itr = tubeGeometries.features
    val cfc = new CompositeFeatureCollection(List[SimpleFeatureCollection]())
    while(itr.hasNext) {
      val sf = itr.next
      val bufferedGeom = sf.getDefaultGeometry
      val bufferedGeomFilter = ff.within(geomProperty, ff.literal(bufferedGeom))

      val minDate = new Date(sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date].getTime - maxTime)
      val maxDate = new Date(sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date].getTime + maxTime)
      val dateProperty = ff.property(tubeFeatures.getSchema.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String])
      val dtgFilter = ff.between(dateProperty, ff.literal(minDate), ff.literal(maxDate))

      val combinedFilter = ff.and(List(query.getFilter, bufferedGeomFilter, dtgFilter, filter))
      cfc.add(source.getFeatures(combinedFilter))
    }
    cfc
//    val features = (0 until tubeGeometries.getNumGeometries).map { i =>
//      val bufferedGeom = tubeGeometries.getGeometryN(i)
//      val bufferedGeomFilter = ff.within(geomProperty, ff.literal(bufferedGeom))
//      val combinedFilter = ff.and(List(query.getFilter, bufferedGeomFilter, dtgFilter, filter))
//      source.getFeatures(combinedFilter)
//    }
//
//    new CompositeFeatureCollection(features).asInstanceOf[SimpleFeatureCollection]
  }



  def getTubeGeometries() = {
    val sortedTube = sortByDate(tubeFeatures)

    val buffered = bufferPoints(sortedTube, calcDistance)
    //new CollectGeometries execute(buffered, new NullProgressListener) union
    buffered
  }

  val ff  = CommonFactoryFinder.getFilterFactory2

  def calcDistance = {
    maxSpeed * maxTime
  }

  def sortByDate(features: SimpleFeatureCollection) = {
    // Get source features and sort
    val dateField = new AttributeExpressionImpl(new NameImpl(Constants.SF_PROPERTY_START_TIME))
    val sortBy = new SortByImpl(dateField, SortOrder.DESCENDING)
    new SortedSimpleFeatureCollection(features, Array(sortBy))
  }

  def getDtgFilter = {
    // Calculate some bounding geometry and time frame based on the source features
    val (minDate, maxDate) = getMinMaxDate(tubeFeatures)
    val dateProperty = ff.property(tubeFeatures.getSchema.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String])
    ff.between(dateProperty, ff.literal(minDate), ff.literal(maxDate))
  }



  def bufferPoints(features: SimpleFeatureCollection, meters: Double) = {
    val itr = features.features()
    val buffered = new ListBuffer[SimpleFeature]
    val builder = new SimpleFeatureBuilder(features.getSchema)

    while(itr.hasNext){
      val sf = itr.next()
      val point = sf.getDefaultGeometry.asInstanceOf[Point]

      val calc = new GeodeticCalculator(features.getSchema.getCoordinateReferenceSystem)
      calc.setStartingGeographicPoint(point.getX, point.getY)
      calc.setDirection(0, meters)
      val dest2D = calc.getDestinationGeographicPoint
      val destPoint = (new GeometryFactory).createPoint(new Coordinate(dest2D.getX, dest2D.getY))
      val bufPoint = point.buffer(point.distance(destPoint))

      builder.init(sf)
      val newSf = builder.buildFeature(sf.getID)
      newSf.setDefaultGeometry(bufPoint)
      buffered += newSf
    }

    new ListFeatureCollection(features.getSchema, buffered)
  }

  def getMinMaxDate(features: SimpleFeatureCollection): (Date, Date) = {
    val minVisitor = new MinVisitor(Constants.SF_PROPERTY_START_TIME)
    features.accepts(minVisitor, new NullProgressListener)
    val minDate = minVisitor.getResult.getValue.asInstanceOf[java.util.Date]

    val maxVisitor = new MaxVisitor(Constants.SF_PROPERTY_START_TIME)
    features.accepts(maxVisitor, new NullProgressListener)
    val maxDate = maxVisitor.getResult.getValue.asInstanceOf[java.util.Date]

    (minDate, maxDate)
  }
}

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult