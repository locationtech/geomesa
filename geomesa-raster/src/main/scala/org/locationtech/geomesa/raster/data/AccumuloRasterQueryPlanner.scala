/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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


package org.locationtech.geomesa.raster.data

import com.google.common.collect.ImmutableSetMultimap
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.hadoop.io.Text
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.process.knn.TouchingGeoHashes
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.locationtech.geomesa.raster.iterators.RasterFilteringIterator
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeohashUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

// TODO: Constructor needs info to create Row Formatter
// right now the schema is not used
// TODO: Consider adding resolutions + extent info  https://geomesa.atlassian.net/browse/GEOMESA-645
case class AccumuloRasterQueryPlanner(schema: RasterIndexSchema) extends Logging with IndexFilterHelpers {

  def modifyHashRange(hash: String, expectedLen: Int, res: String): ARange = expectedLen match {
    // JNH: Think about 0-bit GH some more.
    case 0                                     => new ARange(new Text(s"~$res~"))
    case lucky if expectedLen == hash.length   => new ARange(new Text(s"~$res~$hash"))
    case shorten if expectedLen < hash.length  => new ARange(new Text(s"~$res~${hash.substring(0, expectedLen)}"))
    case lengthen if expectedLen > hash.length => new ARange(new Text(s"~$res~$hash"), new Text(s"~$res~$hash~"))
  }

  def getQueryPlan(rq: RasterQuery, resAndGeoHashMap: ImmutableSetMultimap[Double, Int]): QueryPlan = {
    val availableResolutions = resAndGeoHashMap.keys.toList.distinct.sorted

    // Step 1. Pick resolution

    val selectedRes: Double = selectResolution(rq.resolution, availableResolutions)
    val res = lexiEncodeDoubleToString(selectedRes)

    // Step 2. Pick GeoHashLength
    val GeoHashLenList = resAndGeoHashMap.get(selectedRes).toList
    val expectedGeoHashLen = if (GeoHashLenList.isEmpty) {
      0
    } else {
      GeoHashLenList.max
    }

    // Step 3. Given an expected Length and the query, pad up or down the CAGH
    val closestAcceptableGeoHash = GeohashUtils.getClosestAcceptableGeoHash(rq.bbox)

    val hashes: List[String] = closestAcceptableGeoHash match {
      case Some(gh) =>
        val preliminaryHashes = List(gh.hash)
        if (rq.bbox.equals(gh.bbox) || gh.bbox.covers(rq.bbox)) {
          preliminaryHashes
        } else {
          val touching = TouchingGeoHashes.touching(gh).map(_.hash)
          (preliminaryHashes ++ touching).distinct
        }
      case _ => BoundingBox.getGeoHashesFromBoundingBox(rq.bbox)
    }

    logger.debug(s"RasterQueryPlanner: BBox: ${rq.bbox} has geohashes: $hashes, and has encoded Resolution: $res")
    println(s"Scanning at res: $selectedRes, with hashes: $hashes")
    val r = hashes.map { gh => modifyHashRange(gh, expectedGeoHashLen, res) }.distinct

    // of the Ranges enumerated, get the merge of the overlapping Ranges
    val rows = ARange.mergeOverlapping(r)
    println(s"Scanning with ranges: $rows")

    // setup the RasterFilteringIterator
    val cfg = new IteratorSetting(90, "raster-filtering-iterator", classOf[RasterFilteringIterator])
    configureRasterFilter(cfg, AccumuloRasterQueryPlanner.constructRasterFilter(rq.bbox.geom, indexSFT))
    configureRasterMetadataFeatureType(cfg, indexSFT)

    // TODO: WCS: setup a CFPlanner to match against a list of strings
    // ticket is GEOMESA-559
    QueryPlan(Seq(cfg), rows, Seq())
  }

  def selectResolution(suggestedResolution: Double, availableResolutions: List[Double]): Double = {
    logger.debug(s"RasterQueryPlanner: trying to get resolution $suggestedResolution " +
      s"from available Resolutions: ${availableResolutions.sorted}")
    val ret = availableResolutions match {
      case empty if availableResolutions.isEmpty   => 1.0
      case one if availableResolutions.length == 1 => availableResolutions.head
      case _                                       =>
            val lowerResolutions = availableResolutions.filter(_ <= suggestedResolution)
            logger.debug(s"RasterQueryPlanner: Picking a resolution from: $lowerResolutions")
            lowerResolutions match {
              case Nil => availableResolutions.min
              case _ => lowerResolutions.max
            }
    }
    logger.debug(s"RasterQueryPlanner: Decided to use resolution: $ret")
    ret
  }

  def configureRasterFilter(cfg: IteratorSetting, filter: Filter) = {
    cfg.addOption(DEFAULT_FILTER_PROPERTY_NAME, ECQL.toCQL(filter))
  }

  def configureRasterMetadataFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

}

object AccumuloRasterQueryPlanner {
  val ff = CommonFactoryFinder.getFilterFactory2

  def constructRasterFilter(geom: Geometry, featureType: SimpleFeatureType): Filter = {
    val property = ff.property(featureType.getGeometryDescriptor.getLocalName)
    val bounds = ff.literal(geom)
    ff.and(ff.intersects(property, bounds), ff.not(ff.touches(property, bounds)))
  }

}

case class ResolutionPlanner(ires: Double) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(lexiEncodeDoubleToString(ires)))
}

case class BandPlanner(band: String) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(band))
}
