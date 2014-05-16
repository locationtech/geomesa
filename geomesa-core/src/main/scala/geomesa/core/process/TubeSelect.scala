package geomesa.process

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.index.Constants
import geomesa.core.util.UniqueMultiCollection
import geomesa.process.GapFill.GapFill
import geomesa.utils.geotools.Conversions._
import java.util.Date
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeResult, DescribeParameter, DescribeProcess}
import org.geotools.process.vector.VectorProcess
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
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
                 description = "Max speed of the object in m/s for nofill, straightLine, aroundLand gapfill methods")
               maxSpeed: java.lang.Long,

               @DescribeParameter(
                 name = "maxTime",
                 min = 0,
                 description = "Time as seconds for nofill, straightLine, aroundLand gapfill methods")
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
                 description = "Method of filling gap (nofill, straightLine)")
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
                                      Option(gapFill).map(GapFill.withName(_)).getOrElse(GapFill.NOFILL))
    featureCollection.accepts(tubeVisitor, new NullProgressListener)
    tubeVisitor.getResult.asInstanceOf[TubeResult].results
  }

}

object GapFill extends Enumeration{
  type GapFill = Value
  val NOFILL = Value("nofill")
}

class TubeVisitor(
                   val tubeFeatures: SimpleFeatureCollection,
                   val featureCollection: SimpleFeatureCollection,
                   val filter: Filter = Filter.INCLUDE,
                   val maxSpeed: Long,
                   val maxTime: Long,
                   val bufferSize: Double,
                   val maxBins: Int,
                   val gapFill: GapFill = GapFill.NOFILL
                   ) extends FeatureCalc {

  var resultCalc: TubeResult = null

  def visit(feature: Feature): Unit = {}

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = TubeResult(r)

  val ff  = CommonFactoryFinder.getFilterFactory2

  val bufferDistance =  if(bufferSize > 0) bufferSize else maxSpeed * maxTime

  def tubeSelect(source: SimpleFeatureSource, query: Query): SimpleFeatureCollection = {

    // Create a time binned set of tube features with no gap filling
    val binnedTube = createTubeNoGapFill

    val geomProperty = ff.property(source.getSchema.getGeometryDescriptor.getName)

    val queryResults = binnedTube.map { sf =>
      val sfTime = TubeVisitor.getStartTime(sf).getTime
      val minDate = new Date(sfTime - maxTime)
      val maxDate = new Date(sfTime + maxTime)
      val dateProperty = ff.property(source.getSchema.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String])
      val dtgFilter = ff.between(dateProperty, ff.literal(minDate), ff.literal(maxDate))

      val geom = sf.getDefaultGeometry.asInstanceOf[Geometry]

      // Eventually these can be combined into OR queries and the QueryPlanner can create multiple Accumulo Ranges
      // Buf for now we issue multiple queries
      val geoms = (0 until geom.getNumGeometries).map { i => geom.getGeometryN(i) }
      geoms.flatMap { g =>
        val geomFilter = ff.intersects(geomProperty, ff.literal(g))
        val combinedFilter = ff.and(List(query.getFilter, geomFilter, dtgFilter, filter))
        source.getFeatures(combinedFilter).features
      }
    }

    // Time slices may not be disjoint so we have to buffer results and dedup for now
    new UniqueMultiCollection(source.getSchema, queryResults)
  }

  def createTubeNoGapFill = {
    val dtgField = geomesa.core.data.extractDtgField(tubeFeatures.getSchema)
    val buffered = TubeVisitor.bufferAndTransform(tubeFeatures, bufferDistance, dtgField)
    val sortedTube = buffered.sortBy { sf => TubeVisitor.getStartTime(sf).getTime }
    TubeVisitor.timeBinAndUnion(sortedTube, maxBins)
  }

}

object TubeVisitor {

  val calc = new GeodeticCalculator()
  val geoFac = new GeometryFactory
  val tubeType = DataUtilities.createType("tubeType", Constants.TYPE_SPEC)
  val builder = new SimpleFeatureBuilder(tubeType)

  def metersToDegrees(meters: Double, point: Point) = {
    calc.setStartingGeographicPoint(point.getX, point.getY)
    calc.setDirection(0, meters)
    val dest2D = calc.getDestinationGeographicPoint
    val destPoint = geoFac.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
    point.distance(destPoint)
  }

  def bufferGeom(geom: Geometry, meters: Double) = geom.buffer(metersToDegrees(meters, geom.getCentroid))

  def bufferAndTransform(tubeFeatures: SimpleFeatureCollection, meters: Double, dtgField: String) = tubeFeatures.features.map { sf =>
      val bufferedGeom = bufferGeom(getGeom(sf), meters)
      builder.reset()
      builder.set(Constants.SF_PROPERTY_GEOMETRY, bufferedGeom)

      // warning...may not be a date
      builder.set(Constants.SF_PROPERTY_START_TIME, sf.getAttribute(dtgField))
      builder.set(Constants.SF_PROPERTY_END_TIME, null)
      builder.buildFeature(sf.getID)
    }.toSeq

  // Bin ordered features into maxBins that retain order by date then union by geometry
  def timeBinAndUnion(features: Seq[SimpleFeature], maxBins: Int) = {
    val numFeatures = features.size
    val binSize =
      if(maxBins > 0 )
        numFeatures / maxBins + (if (numFeatures % maxBins == 0 ) 0 else 1)
      else
        numFeatures

    features.grouped(binSize).zipWithIndex.map { case(bin, idx) => unionFeatures(bin, idx.toString) }
  }

  // Union features to create a single geometry and single combined time range
  def unionFeatures(orderedFeatures: Seq[SimpleFeature], id: String) = {
    val geoms = orderedFeatures.map { sf => getGeom(sf) }
    val unionGeom = geoFac.buildGeometry(geoms).union
    val min = getStartTime(orderedFeatures(0))
    val max = getStartTime(orderedFeatures(orderedFeatures.size - 1))

    builder.reset()
    builder.buildFeature(id, Array(unionGeom, min, max))
  }

  def getStartTime(sf: SimpleFeature) = sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date]

  def getGeom(sf: SimpleFeature) = sf.getDefaultGeometry.asInstanceOf[Geometry]

}

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult