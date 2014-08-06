package geomesa.core.process.knn

import collection.JavaConverters._
import com.vividsolutions.jts.geom.GeometryFactory
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.utils.geotools.Conversions._
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{CalcResult, FeatureCalc, AbstractCalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Geomesa-enabled K Nearest Neighbor Search",
  description = "Performs a K Nearest Neighbor search on a Geomesa feature collection using another feature collection as input"
)
class KNearestNeighborSearchProcess {

  private val log = Logger.getLogger(classOf[KNearestNeighborSearchProcess])

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "Input feature collection that defines the KNN search")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "The data set to query for matching features")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "numDesired",
                 description = "K: number of nearest neighbors to return")
               numDesired: java.lang.Integer,

               @DescribeParameter(
                 name = "estimatedDistance",
                 description = "Estimate of Search Distance in meters for K neighbors---used to set the granularity of the search")
               estimatedDistance: java.lang.Double,

               @DescribeParameter(
                 name = "maxSearchDistance",
                 description = "Maximum search distance in meters---used to prevent runaway queries of the entire table")
               maxSearchDistance: java.lang.Double
               ): SimpleFeatureCollection = {

    log.info("Attempting Geomesa K-Nearest Neighbor Search on collection type " + dataFeatures.getClass.getName)

    if(!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support geomesa KNN search: "+dataFeatures.getClass.getName)
    }
    if(dataFeatures.isInstanceOf[ReTypingFeatureCollection]) {
      log.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }
    val visitor = new KNNVisitor(inputFeatures, dataFeatures, numDesired, estimatedDistance, maxSearchDistance)
    dataFeatures.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[KNNResult].results
  }
}

/**
 *  The main visitor class for the KNN search process
 */

class KNNVisitor( inputFeatures: SimpleFeatureCollection,
                               dataFeatures: SimpleFeatureCollection,
                               numDesired: java.lang.Integer,
                               estimatedDistance: java.lang.Double,
                               maxSearchDistance: java.lang.Double
                             ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[KNNVisitor])

  val geoFac = new GeometryFactory
  val ff = CommonFactoryFinder.getFilterFactory2

  var manualFilter: Filter = _
  val manualVisitResults = new DefaultFeatureCollection(null, dataFeatures.getSchema)

  // called for non AccumuloFeatureCollections
  // does this warrant an actual implementation?
  def visit(feature: Feature): Unit = {}

  var resultCalc: KNNResult = new KNNResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = KNNResult(r)

  /** The KNN-Search interface for the WPS process.
    *
    * Takes as input a Query and SimpleFeatureSource, in addition to
    * inputFeatures which define one or more SimpleFeatures for which to find KNN of each
    *
    * Note that the results are NOT de-duplicated!
    *
    */
  def kNNSearch(source: SimpleFeatureSource, query: Query) = {
    log.info("Running Geomesa K-Nearest Neighbor Search on source type " + source.getClass.getName)
    // create a new FeatureCollection, and add to it the results of a KNN query for each point in inputFeatures
    // the SimpleFeatures are extracted from the (SimpleFeature,distance) tuple here

    // approach #1
    /**
    new DefaultFeatureCollection {
      inputFeatures.features.foreach {
        aFeatureForSearch => addAll(
          KNNQuery.runNewKNNQuery(source, query, numDesired, estimatedDistance, maxSearchDistance, aFeatureForSearch).map{_._1 }.asJavaCollection

        )
      }
    }
    **/
    //approach #2 using a for loop. More readable .......
    new DefaultFeatureCollection {
      for {
        aFeatureForSearch <- inputFeatures.features
        knnResults = KNNQuery.runNewKNNQuery(source, query, numDesired, estimatedDistance, maxSearchDistance, aFeatureForSearch)
        // extract the SimpleFeatures and convert to a Collection. Note that the ordering will not be preserved.
        list = knnResults.map{_._1}.asJavaCollection  // the map extracts the SimpleFeature from the tuple
        _ = addAll(list)
      } {}
    }
  }
}
case class KNNResult(results: SimpleFeatureCollection) extends AbstractCalcResult
