package geomesa.core.process.knn

import geomesa.core.filter._
import geomesa.utils.geohash.GeoHash
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.SimpleFeature
import scala.annotation.tailrec

/**
 * This object contains the main algorithm for the GeoHash-based iterative KNN search.
 */


object KNNQuery {
  /**
   * Method to kick off a new KNN query about aFeatureForSearch
   */
  def runNewKNNQuery(source: SimpleFeatureSource,
                     query: Query,
                     numDesired: Int,
                     searchDistance: Double,
                     maxDistance: Double,
                     aFeatureForSearch: SimpleFeature): BoundedNearestNeighbors[(SimpleFeature, Double)] = {

    // setup the GeoHashSpiral -- it requires the search point,
    // an estimate of the area containing the K Nearest Neighbors,
    // and a maximum distance for search as a safeguard
    val geoHashPQ = GeoHashSpiral(aFeatureForSearch, searchDistance, maxDistance)

    // setup the NearestNeighbors PriorityQueue -- this is the last usage of aFeatureForSearch
    val sfPQ = NearestNeighbors(aFeatureForSearch, numDesired)

    // begin the search with the recursive method
    runKNNQuery(source, query, geoHashPQ, sfPQ)
  }

  /**
   * Recursive function to iteratively query a number of geohashes and insert their results into a
   * NearestNeighbors priority queue
   */
  @tailrec
  def runKNNQuery(source: SimpleFeatureSource,
                   query: Query,
                   ghPQ: GeoHashSpiral,
                   sfPQ: BoundedNearestNeighbors[(SimpleFeature,Double)]) : BoundedNearestNeighbors[(SimpleFeature,Double)] = {
    import geomesa.utils.geotools.Conversions.toRichSimpleFeatureIterator

    if (!ghPQ.hasNext) sfPQ
    else {
        val newGH = ghPQ.next()
        // copy the query in order to pass the original to the next recursion
        val newQuery = generateKNNQuery(newGH, query, source)

        val newFeatures = source.getFeatures(newQuery).features

        // insert the SimpleFeature and its distance into sfPQ
        newFeatures.foreach{ sf:SimpleFeature => sfPQ.enqueue( (sf,sfPQ.distance(sf)) ) }

        // apply filter to ghPQ if we've found k neighbors
        if (sfPQ.isFull) sfPQ.maxDistance.foreach { x: Double => ghPQ.mutateFilterDistance(x)}
        //println ("KNN Status:" + newGH.hash + " " + sfPQ.maxDistance.getOrElse(0.0) +" " + sfPQ.length )
        runKNNQuery(source, query, ghPQ, sfPQ)
    }
  }

  /**
   * Generate a new query by narrowing another down to a single GeoHash
   */
  def generateKNNQuery(gh: GeoHash, oldQuery: Query, source: SimpleFeatureSource): Query = {

    // setup a new BBOX filter to add to the original suite
    val geomProp = ff.property(source.getSchema.getGeometryDescriptor.getName)

    val newGHEnv = new ReferencedEnvelope(gh.bbox, oldQuery.getCoordinateSystem)

    val newGHFilter = ff.bbox(geomProp, newGHEnv)

    // could ALSO apply a dwithin filter if k neighbors have been found.
    // copy the original query before mutation, then AND the new GeoHash filter with the original filter
    new Query(oldQuery) { setFilter(ff.and(oldQuery.getFilter, newGHFilter)) }
  }
}
