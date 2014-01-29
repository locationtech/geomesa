/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package geomesa.core.data

import collection.immutable.TreeSet
import com.vividsolutions.jts.geom._
import geomesa.core.data.FilterToAccumulo.{SetLikeFilter, SetLikeInterval, SetLikePolygon, SetLikeExtraction}
import geomesa.core.index._
import geomesa.utils.geohash.GeohashUtils
import org.apache.accumulo.core.data.Value
import org.geotools.data.{Query, FeatureReader}
import org.geotools.filter.text.ecql.ECQL
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class AccumuloFeatureReader(dataStore: AccumuloDataStore,
                             featureName: String,
                             query: Query,
                             indexSchemaFmt: String,
                             attributes: String,
                             sft: SimpleFeatureType)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] {

  import AccumuloFeatureReader._

  lazy val indexSchema = SpatioTemporalIndexSchema(indexSchemaFmt, sft)

  lazy val bounds = dataStore.getBounds(query) match {
    case null => null
    case b =>
      val res = latLonGeoFactory.toGeometry(b)
      if(res.isInstanceOf[Point] || res.isInstanceOf[LineString]) res.buffer(0.01).asInstanceOf[Polygon]
      else res.asInstanceOf[Polygon]
  }

  lazy val (iterValues,bs) =
    if(query.getFilter == Filter.EXCLUDE) emptyResultsSet
    else try {
      val givenFilter = query.getFilter

      // extract the query polygon, the query interval, and the filter that
      // results from removing these portions
      val extractor = FilterExtractor(
        SF_PROPERTY_GEOMETRY,
        TreeSet(SF_PROPERTY_START_TIME, SF_PROPERTY_END_TIME))
      val Extraction(optPolygon, optInterval, optFilter) = {
        val extraction = extractor.extractAndModify(givenFilter)
        if (SetLikeExtraction.isDefined(extraction)) extraction.get
        else Extraction(
          SetLikePolygon.nothing,
          SetLikeInterval.nothing,
          SetLikeFilter.nothing
        )
      }

      // compute the net query-polygon, interval, and ECQL filter
      val (polygon, isDisjoint) = optPolygon match {
        case op if SetLikePolygon.isUndefined(op)  => (null, false)
        case op if SetLikePolygon.isNothing(op)    => (null, true)
        case op if SetLikePolygon.isEverything(op) => (null, false)
        case Some(rawPoly)                         => bounds match {
          case null                          => (rawPoly, false)
          case b if rawPoly.disjoint(bounds) => (null, true)
          case b if rawPoly.covers(bounds)   => (bounds.asInstanceOf[Polygon], false)
          case _                             =>
            (rawPoly.intersection(bounds).asInstanceOf[Polygon], false)
        }
      }
      val interval = optInterval match {
        case oi if SetLikeInterval.isUndefined(oi)  => null
        case oi if SetLikeInterval.isNothing(oi)    => null
        case oi if SetLikeInterval.isEverything(oi) => null
        case Some(i)                                => i
      }
      val ecql: Option[String] = optFilter match {
        case of if SetLikeFilter.isUndefined(of)  => None
        case of if SetLikeFilter.isNothing(of)    => None
        case of if SetLikeFilter.isEverything(of) => None
        case Some(filter)                         => Some(ECQL.toCQL(filter))
      }

      // how large a query polygon do we have?
      val ghPrecision= GeohashUtils.estimateGeometryGeohashPrecision(polygon)

      //@TODO convert this to DEBUG logging as part of CBGEO-34
      println("[AccumuloFeatureReader]")
      println("  Query:  " + query.toString)
      println("  Polygon:  " + polygon)
      println("  Are query-polygon and bounds disjoint?  " + isDisjoint)
      println(s"  Interval:  $interval")
      println("  GeoHash precision:  " + ghPrecision)
      println("  ECQL:  " + ecql)

      // run the query
      val bs = dataStore.createBatchScanner

      val iter =  if(isDisjoint) emptyValueIterator
                  else           indexSchema.query(bs, polygon, interval, attributes, ecql)

      ( iter, Some(bs) )
    } catch {
      case t: Throwable => {
        // let the user (perusing the log, no doubt) see why the query failed
        //@TODO convert this to DEBUG logging as part of CBGEO-34
        println("Could not satisfy query request", t)

        // dummy return value
        emptyResultsSet
      }
    }

  override def getFeatureType = sft

  override def next() = SimpleFeatureEncoder.decode(getFeatureType, iterValues.next())

  override def hasNext = iterValues.hasNext

  override def close() = bs.foreach(_.close())
}

object AccumuloFeatureReader {
  val latLonGeoFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)

  // used when the query-polygon is disjoint with the known feature bounds
  def emptyValueIterator : Iterator[Value] = List[Value]().iterator

  // useful when a query should return nothing
  private def emptyResultsSet = (emptyValueIterator, None)
}