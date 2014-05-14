package geomesa.process

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.index.Constants
import geomesa.core.util.{SFCIterator, UniqueMultiCollection}
import geomesa.process.GapFill.GapFill
import java.util.{UUID, Date}
import org.apache.log4j.Logger
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.{DataUtilities, Query}
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
                 name = "maxSpeed",
                 min = 0,
                 description = "Max speed of the object in m/s for none, straightLine, aroundLand gapfill methods")
               maxSpeed: java.lang.Long,

               @DescribeParameter(
                 name = "maxTime",
                 min = 0,
                 description = "Time as seconds for none, straightLine, aroundLand gapfill methods")
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
                 description = "Method of filling gap (none, straightLine)")
               gapFill: String

               ): SimpleFeatureCollection = {

    // assume for now that firstFeatures is a singleton collection
    val tubeVisitor = new TubeVisitor(
                                      tubeFeatures,
                                      featureCollection,
                                      Option(filter).getOrElse(Filter.INCLUDE),
                                      Option(maxSpeed).getOrElse(0).asInstanceOf[Long],
                                      Option(maxTime).getOrElse(0).asInstanceOf[Long],
                                      Option(bufferSize).getOrElse(0).asInstanceOf[Double],
                                      Option(maxBins).getOrElse(0).asInstanceOf[Int],
                                      GapFill.withName(Option(gapFill).getOrElse(GapFill.NONE.toString)))
    featureCollection.accepts(tubeVisitor, new NullProgressListener)
    tubeVisitor.getResult.asInstanceOf[TubeResult].results
  }

}

object GapFill extends Enumeration{
  type GapFill = Value
  val NONE = Value("none")
}

class TubeVisitor(
                   val tubeFeatures: SimpleFeatureCollection,
                   val featureCollection: SimpleFeatureCollection,
                   val filter: Filter = Filter.INCLUDE,
                   val maxSpeed: Long,
                   val maxTime: Long,
                   val bufferSize: Double,
                   val maxBins: Int,
                   val gapFill: GapFill = GapFill.NONE
                   ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[TubeVisitor])

  var resultCalc: TubeResult = null

  def visit(feature: Feature): Unit = {}

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = TubeResult(r)

  val ff  = CommonFactoryFinder.getFilterFactory2

  def bufferDistance: Double = if(bufferSize > 0) bufferSize else maxSpeed * maxTime

  def tubeSelect(source: SimpleFeatureSource, query: Query): SimpleFeatureCollection = {

    val binnedTube = gapFill match {
      case _ => createTubeNoGap
    }

    val geomProperty = ff.property(source.getSchema.getGeometryDescriptor.getName)

    val queryResults = new ListBuffer[SimpleFeatureCollection]

    new SFCIterator(binnedTube).foreach { sf =>
      val minDate = new Date(sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date].getTime - maxTime)
      val maxDate = new Date(sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date].getTime + maxTime)
      val dateProperty = ff.property(source.getSchema.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String])
      val dtgFilter = ff.between(dateProperty, ff.literal(minDate), ff.literal(maxDate))

      val geom = sf.getDefaultGeometry.asInstanceOf[Geometry]

      // Eventually these can be combined into OR queries and the QueryPlanner can create multiple Accumulo Ranges
      // Buf for now we issue multiple queries
      (0 until geom.getNumGeometries).map { i =>
        val geomFilter = ff.intersects(geomProperty, ff.literal(geom.getGeometryN(i)))
        val combinedFilter = ff.and(List(query.getFilter, geomFilter, dtgFilter, filter))
        queryResults += source.getFeatures(combinedFilter)
      }
    }

    // Time slices may not be disjoint so we have to buffer results and dedup for now
    new UniqueMultiCollection(source.getSchema, queryResults)
  }

  def createTubeNoGap = {
    val buffered = TubeVisitor.bufferAndTransform(tubeFeatures, bufferDistance)
    val sortedTube = TubeVisitor.sortByDate(buffered)
    TubeVisitor.timeBinAndUnion(sortedTube, maxBins)
  }

}

object TubeVisitor {

  val calc = new GeodeticCalculator()
  val geoFac = new GeometryFactory
  val tubeType = DataUtilities.createType("tubeType", Constants.TYPE_SPEC)

  def metersToDegrees(meters: Double, point: Point) = {
    calc.setStartingGeographicPoint(point.getX, point.getY)
    calc.setDirection(0, meters)
    val dest2D = calc.getDestinationGeographicPoint
    val destPoint = geoFac.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
    point.distance(destPoint)
  }

  def bufferGeom(geom: Geometry, meters: Double) = geom.buffer(metersToDegrees(meters, geom.getCentroid))

  def bufferAndTransform(features: SimpleFeatureCollection, meters: Double) = {
    val buffered = new ListBuffer[SimpleFeature]
    val builder = new SimpleFeatureBuilder(tubeType)

    new SFCIterator(features).foreach { sf =>
      builder.reset()
      builder.set(Constants.SF_PROPERTY_GEOMETRY, bufferGeom(sf.getDefaultGeometry.asInstanceOf[Geometry], meters))
      builder.set(Constants.SF_PROPERTY_START_TIME, sf.getAttribute(Constants.SF_PROPERTY_START_TIME))
      builder.set(Constants.SF_PROPERTY_END_TIME, null)
      buffered += builder.buildFeature(sf.getID)
    }

    new ListFeatureCollection(tubeType, buffered)
  }

  def sortByDate(features: SimpleFeatureCollection) = {
    val dateField = new AttributeExpressionImpl(new NameImpl(Constants.SF_PROPERTY_START_TIME))
    val sortBy = new SortByImpl(dateField, SortOrder.DESCENDING)
    new SortedSimpleFeatureCollection(features, Array(sortBy))
  }

  // Bin ordered features into maxBins number of bins by filling bins to a maxBinSize
  def timeBinAndUnion(features: SimpleFeatureCollection, maxBins: Int): SimpleFeatureCollection = {
    val numFeatures = features.size
    val binSize =
      if(maxBins > 0 )
        numFeatures / maxBins + (if (numFeatures % maxBins == 0 ) 0 else 1)
      else
        numFeatures

    val bins = collection.mutable.HashMap.empty[Int, ListFeatureCollection]
    var bin = 0

    def isFull = bins.contains(bin) && bins(bin).size >= binSize

    new SFCIterator(features).foreach { sf =>
      if(isFull) bin += 1

      if(bins.contains(bin))
        bins.get(bin).get.add(sf)
      else
        bins.put(bin, new ListFeatureCollection(features.getSchema, new ListBuffer[SimpleFeature]+=sf))
    }

    val builder = new SimpleFeatureBuilder(features.getSchema)
    val binned = bins.map { case (k,v) =>
      val unionGeom = (new CollectGeometries execute(v, new NullProgressListener)).union
      val min = dateVisit(new MinVisitor(Constants.SF_PROPERTY_START_TIME), v)
      val max = dateVisit(new MaxVisitor(Constants.SF_PROPERTY_START_TIME), v)
      builder.reset()
      builder.set(Constants.SF_PROPERTY_GEOMETRY, unionGeom)
      builder.set(Constants.SF_PROPERTY_START_TIME, min)
      builder.set(Constants.SF_PROPERTY_END_TIME, max)
      builder.buildFeature(UUID.randomUUID().toString)
    }

    new ListFeatureCollection(features.getSchema, binned.toList)
  }

  def dateVisit(v: FeatureCalc, features: SimpleFeatureCollection) = {
    features.accepts(v, new NullProgressListener)
    v.getResult.getValue.asInstanceOf[java.util.Date]
  }

}

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult