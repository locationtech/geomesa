package geomesa.core.process.tube

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.core.index.Constants
import geomesa.core.process.tube.GapFill.GapFill
import geomesa.core.util.UniqueMultiCollection
import geomesa.utils.geotools.Conversions._
import java.util.Date
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.{ReTypingFeatureCollection, EmptyFeatureCollection}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeResult, DescribeParameter, DescribeProcess}
import org.geotools.process.vector.VectorProcess
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Tube Select",
  description = "Performs a tube select on a Geomesa feature collection based on another feature collection"
)
class TubeSelectProcess {

  private val log = Logger.getLogger(classOf[TubeSelectProcess])

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

    if(featureCollection.isInstanceOf[ReTypingFeatureCollection]) {
      log.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
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

    log.info("Visiting source type: "+source.getClass.getName)

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

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult