package geomesa.process

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.core.index.Constants
import geomesa.core.util.UniqueMultiCollection
import geomesa.process.GapFill.GapFill
import geomesa.utils.geotools.Conversions._
import java.util.Date
import org.apache.log4j.Logger
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.EmptyFeatureCollection
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
import org.joda.time.format.DateTimeFormat
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import java.util.concurrent.atomic.AtomicInteger
import geomesa.utils.text.WKTUtils

@DescribeProcess(
  title = "Performs a tube select on one feature collection based on another feature collection",
  description = "Returns a feature collection"
)
class TubeSelect extends VectorProcess {

  private val log = Logger.getLogger(classOf[TubeSelect])

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
                 description = "Method of filling gap (nofill, line)")
               gapFill: String

               ): SimpleFeatureCollection = {

    log.info("Tube selecting on collection type "+featureCollection.getClass.getName)

    // assume for now that firstFeatures is a singleton collection
    val tubeVisitor = new TubeVisitor(
                                      tubeFeatures,
                                      featureCollection,
                                      Option(filter).getOrElse(Filter.INCLUDE),
                                      Option(maxSpeed).getOrElse(0L).asInstanceOf[Long],
                                      Option(maxTime).getOrElse(0L).asInstanceOf[Long],
                                      Option(bufferSize).getOrElse(0.0).asInstanceOf[Double],
                                      Option(maxBins).getOrElse(0).asInstanceOf[Int],
                                      Option(gapFill).map(GapFill.withName(_)).getOrElse(GapFill.NOFILL))

    if(!featureCollection.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided feature collection type may not support tubing: "+featureCollection.getClass.getName)
    }

    featureCollection.accepts(tubeVisitor, new NullProgressListener)
    tubeVisitor.getResult.asInstanceOf[TubeResult].results
  }

}

object GapFill extends Enumeration{
  type GapFill = Value
  val NOFILL = Value("nofill")
  val LINE = Value("line")
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

  private val log = Logger.getLogger(classOf[TubeVisitor])

  var resultCalc: TubeResult = new TubeResult(new EmptyFeatureCollection(featureCollection.getSchema))

  def visit(feature: Feature): Unit = {}

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = TubeResult(r)

  val ff  = CommonFactoryFinder.getFilterFactory2

  val bufferDistance =  if(bufferSize > 0) bufferSize else maxSpeed * maxTime

  def tubeSelect(source: SimpleFeatureSource, query: Query): SimpleFeatureCollection = {

    log.info("Visting source type: "+source.getClass.getName)

    val geomProperty = ff.property(source.getSchema.getGeometryDescriptor.getName)
    val dateProperty = ff.property(source.getSchema.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String])

    if(log.isDebugEnabled) log.debug("Querying with date property: "+dateProperty)
    if(log.isDebugEnabled) log.debug("Querying with geometry property: "+geomProperty)

    // Create a time binned set of tube features with no gap filling

    val tubeBuilder = gapFill match {
      case GapFill.LINE => new LineGapFill(tubeFeatures, bufferDistance, maxBins)
      case _ => new NoGapFill(tubeFeatures, bufferDistance, maxBins)
    }

    val tube = tubeBuilder.createTube

    val queryResults = tube.map { sf =>
      val sfMin = tubeBuilder.getStartTime(sf).getTime
      val minDate = new Date(sfMin - maxTime*1000)

      val sfMax = tubeBuilder.getEndTime(sf).getTime
      val maxDate = new Date(sfMax + maxTime*1000)

      val dtg1 = ff.greater(dateProperty, ff.literal(minDate))
      val dtg2 = ff.less(dateProperty, ff.literal(maxDate))

      val geom = sf.getDefaultGeometry.asInstanceOf[Geometry]

      // Eventually these can be combined into OR queries and the QueryPlanner can create multiple Accumulo Ranges
      // Buf for now we issue multiple queries
      val geoms = (0 until geom.getNumGeometries).map { i => geom.getGeometryN(i) }
      geoms.flatMap { g =>
        val geomFilter = ff.intersects(geomProperty, ff.literal(g))
        val combinedFilter = ff.and(List(query.getFilter, geomFilter, dtg1, dtg2, filter))
        source.getFeatures(combinedFilter).features
      }
    }

    // Time slices may not be disjoint so we have to buffer results and dedup for now
    new UniqueMultiCollection(source.getSchema, queryResults)
  }

}

abstract class TubeBuilder(val tubeFeatures: SimpleFeatureCollection,
                           val bufferDistance: Double,
                           val maxBins: Int) {

  private val log = Logger.getLogger(classOf[TubeBuilder])

  val calc = new GeodeticCalculator()
  val dtgField = geomesa.core.data.extractDtgField(tubeFeatures.getSchema)
  val geoFac = new GeometryFactory
  val tubeType = DataUtilities.createType("tubeType", Constants.TYPE_SPEC)
  val builder = new SimpleFeatureBuilder(tubeType)

  // default to ISO 8601 date format
  val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  def getStartTime(sf: SimpleFeature) = sf.getAttribute(Constants.SF_PROPERTY_START_TIME).asInstanceOf[Date]

  def getEndTime(sf: SimpleFeature) = sf.getAttribute(Constants.SF_PROPERTY_END_TIME).asInstanceOf[Date]

  def getGeom(sf: SimpleFeature) = sf.getDefaultGeometry.asInstanceOf[Geometry]

  def bufferGeom(geom: Geometry, meters: Double) = geom.buffer(metersToDegrees(meters, geom.getCentroid))

  def metersToDegrees(meters: Double, point: Point) = {
    if(log.isDebugEnabled) log.debug("Buffering: "+meters.toString + " "+WKTUtils.write(point))

    calc.setStartingGeographicPoint(point.getX, point.getY)
    calc.setDirection(0, meters)
    val dest2D = calc.getDestinationGeographicPoint
    val destPoint = geoFac.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
    point.distance(destPoint)
  }

  def buffer(simpleFeatures: Iterator[SimpleFeature], meters:Double) = simpleFeatures.map { sf =>
    val bufferedGeom = bufferGeom(getGeom(sf), meters)
    builder.reset()
    builder.init(sf)
    builder.set(Constants.SF_PROPERTY_GEOMETRY, bufferedGeom)
    builder.buildFeature(sf.getID)
  }

  // transform the input tubeFeatures into the intermediate SF used by the
  // tubing code consisting of three attributes (geom, startTime, endTime)
  //
  // handle date parsing from input -> TODO revisit date parsing...
  def transform(tubeFeatures: SimpleFeatureCollection,
                dtgField: String): Iterator[SimpleFeature] = tubeFeatures.features().map { sf =>
    val date =
      if(sf.getAttribute(dtgField).isInstanceOf[String])
        df.parseDateTime(sf.getAttribute(dtgField).asInstanceOf[String]).toDate
      else sf.getAttribute(dtgField)

    builder.reset()
    builder.buildFeature(sf.getID, Array(sf.getDefaultGeometry, date, null))
  }

  def createTube: Iterator[SimpleFeature]
}

class NoGapFill(tubeFeatures: SimpleFeatureCollection,
                bufferDistance: Double,
                maxBins: Int) extends TubeBuilder(tubeFeatures, bufferDistance, maxBins) {

  private val log = Logger.getLogger(classOf[NoGapFill])

  // Bin ordered features into maxBins that retain order by date then union by geometry
  def timeBinAndUnion(features: Iterable[SimpleFeature], maxBins: Int) = {
    val numFeatures = features.size
    val binSize =
      if(maxBins > 0 )
        numFeatures / maxBins + (if (numFeatures % maxBins == 0 ) 0 else 1)
      else
        numFeatures

    features.grouped(binSize).zipWithIndex.map { case(bin, idx) => unionFeatures(bin.toSeq, idx.toString) }
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

  override def createTube = {
    log.info("Creating tube with no gap filling")

    val transformed = transform(tubeFeatures, dtgField)
    val buffered = buffer(transformed, bufferDistance)
    val sortedTube = buffered.toSeq.sortBy { sf => getStartTime(sf).getTime }
    timeBinAndUnion(sortedTube, maxBins)
  }
}

class LineGapFill(tubeFeatures: SimpleFeatureCollection,
                  bufferDistance: Double,
                  maxBins: Int) extends TubeBuilder(tubeFeatures, bufferDistance, maxBins) {

  private val log = Logger.getLogger(classOf[LineGapFill])

  val id = new AtomicInteger(0)

  def nextId = id.getAndIncrement.toString

  override def createTube = {
    log.info("Creating tube with line gap fill")

    val transformed = transform(tubeFeatures, dtgField)
    val sortedTube = transformed.toSeq.sortBy { sf => getStartTime(sf).getTime }

    val lineFeatures = sortedTube.sliding(2).map { pair =>
      val p1 = getGeom(pair(0)).getCentroid
      val t1 = getStartTime(pair(0))
      val p2 = getGeom(pair(1)).getCentroid
      val t2 = getStartTime(pair(1))

      val geo =
        if(p1.equals(p2)) p1
        else new LineString(new CoordinateArraySequence(Array(p1.getCoordinate, p2.getCoordinate)), geoFac)

      if(log.isDebugEnabled) log.debug("Created Line-filled Geometry: " + WKTUtils.write(geo) + " from "
        + WKTUtils.write(getGeom(pair(0))) + " and "
        + WKTUtils.write(getGeom(pair(1))))

      builder.reset
      builder.buildFeature(nextId, Array(geo, t1, t2))
    }

    buffer(lineFeatures, bufferDistance)
  }

}

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult