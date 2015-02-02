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

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.hadoop.io.Text
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.locationtech.geomesa.raster.iterators.RasterFilteringIterator
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash, GeohashUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

// TODO: Constructor needs info to create Row Formatter
// right now the schema is not used
// TODO: Consider adding resolutions + extent info  https://geomesa.atlassian.net/browse/GEOMESA-645
case class AccumuloRasterQueryPlanner(schema: RasterIndexSchema) extends Logging with IndexFilterHelpers {

  def getQueryPlan(rq: RasterQuery, availableResolutions: List[Double]): QueryPlan = {

    // TODO: WCS: Improve this if possible
    // ticket is GEOMESA-560
    // note that this will only go DOWN in GeoHash resolution -- the enumeration will miss any GeoHashes
    // that perfectly match the bbox or ones that fully contain it.
    val closestAcceptableGeoHash = GeohashUtils.getClosestAcceptableGeoHash(rq.bbox).getOrElse(GeoHash("")).hash
    val hashes = (BoundingBox.getGeoHashesFromBoundingBox(rq.bbox) :+ closestAcceptableGeoHash).toSet.toList
    val res = getLexicodedResolution(rq.resolution, availableResolutions)
    logger.debug(s"RasterQueryPlanner: BBox: ${rq.bbox} has geohashes: $hashes, and has encoded Resolution: $res")

    val rows = hashes.map { gh =>
      // TODO: leverage the RasterIndexSchema to construct the range.
      new org.apache.accumulo.core.data.Range(new Text(s"~$res~$gh"))
    }

    // setup the RasterFilteringIterator
    val cfg = new IteratorSetting(9001, "raster-filtering-iterator", classOf[RasterFilteringIterator])
    configureRasterFilter(cfg, constructFilter(getReferencedEnvelope(rq.bbox), indexSFT))
    configureRasterMetadataFeatureType(cfg, indexSFT)

    // TODO: WCS: setup a CFPlanner to match against a list of strings
    // ticket is GEOMESA-559
    QueryPlan(Seq(cfg), rows, Seq())
  }

  def getLexicodedResolution(suggestedResolution: Double, availableResolutions: List[Double]): String =
    lexiEncodeDoubleToString(getResolution(suggestedResolution, availableResolutions))

  def getResolution(suggestedResolution: Double, availableResolutions: List[Double]): Double = {
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

  def constructFilter(ref: ReferencedEnvelope, featureType: SimpleFeatureType): Filter = {
    val ff = CommonFactoryFinder.getFilterFactory2
    val b = ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), ref)
    b.asInstanceOf[Filter]
  }

  def configureRasterFilter(cfg: IteratorSetting, filter: Filter) = {
    cfg.addOption(DEFAULT_FILTER_PROPERTY_NAME, ECQL.toCQL(filter))
  }

  def configureRasterMetadataFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  def getReferencedEnvelope(bbox: BoundingBox): ReferencedEnvelope = {
    val env = bbox.envelope
    new ReferencedEnvelope(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY, DefaultGeographicCRS.WGS84)
  }

}

case class ResolutionPlanner(ires: Double) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(lexiEncodeDoubleToString(ires)))
}

case class BandPlanner(band: String) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(band))
}
