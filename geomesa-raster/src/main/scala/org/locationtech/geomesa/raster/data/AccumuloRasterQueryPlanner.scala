/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.raster.data

import com.google.common.collect.{ImmutableMap, ImmutableSetMultimap}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.hadoop.io.Text
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.index.{IndexFilterHelpers, QueryPlan, _}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.accumulo.process.knn.TouchingGeoHashes
import org.locationtech.geomesa.raster.iterators.{RasterFilteringIterator => RFI}
import org.locationtech.geomesa.raster.{defaultResolution, lexiEncodeDoubleToString, rasterSft, rasterSftName}
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeohashUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.Try

object AccumuloRasterQueryPlanner extends Logging with IndexFilterHelpers {

  def improvedOverlaps(a: Envelope, b: Envelope): Boolean = a.intersects(b) && a.intersection(b).getArea != 0.0

  def getAcceptableResolution(rq: RasterQuery, resAndBoundsMap: ImmutableMap[Double, BoundingBox]): Option[Double] = {
    val availableResolutions = resAndBoundsMap.keySet().toList.sorted
    val preferredRes: Double = selectResolution(rq.resolution, availableResolutions)
    getCourserBounds(rq.bbox, preferredRes, resAndBoundsMap)
  }

  @tailrec
  def getCourserBounds(qb: BoundingBox, r: Double, rToB: ImmutableMap[Double, BoundingBox]): Option[Double] = {
    // get finest bounds that satisfies our needs
    if (rToB.containsKey(r)) {
      if (improvedOverlaps(qb.envelope, rToB.get(r).envelope)) Some(r) else {
        val cr = rToB.keys.filter(_ > r)
        if (cr.nonEmpty) getCourserBounds(qb, cr.min, rToB) else None
      }
    } else None
  }

  def getQueryPlan(rq: RasterQuery, resAndGeoHashMap: ImmutableSetMultimap[Double, Int],
                    resAndBoundsMap: ImmutableMap[Double, BoundingBox]): Option[QueryPlan] = {
    // Step 1. Pick resolution and Make sure the query extent is contained in the extent at that resolution
    val selectedRes: Double = getAcceptableResolution(rq, resAndBoundsMap).getOrElse(defaultResolution)
    val res = lexiEncodeDoubleToString(selectedRes)

    // Step 2. Pick GeoHashLength
    val GeoHashLenList = resAndGeoHashMap.get(selectedRes).toList
    val expectedGeoHashLen = if (GeoHashLenList.isEmpty) 0 else GeoHashLenList.max

    // Step 3. Given an expected Length and the query, pad up or down the CAGH
    val closestAcceptableGeoHash = GeohashUtils.getClosestAcceptableGeoHash(rq.bbox)

    val hashes: List[String] = closestAcceptableGeoHash match {
      case Some(gh) =>
        val preliminaryGeoHash = List(gh.hash)
        if (rq.bbox.equals(gh.bbox) || gh.bbox.covers(rq.bbox)) {
          preliminaryGeoHash
        } else {
          val touching = TouchingGeoHashes.touching(gh).map(_.hash)
          (preliminaryGeoHash ++ touching).distinct
        }
      case _ => Try(BoundingBox.getGeoHashesFromBoundingBox(rq.bbox)) getOrElse List.empty[String]
    }

    // Step 4. Arrive at final ranges
    val r = hashes.map { gh => modifyHashRange(gh, expectedGeoHashLen, res) }.distinct

    if (r.isEmpty) {
      logger.warn(s"AccumuloRasterQueryPlanner: Query was invalid given RasterQuery: $rq")
      None
    } else {
      // of the Ranges enumerated, get the merge of the overlapping Ranges
      val rows = ARange.mergeOverlapping(r)
      logger.debug(s"AccumuloRasterQueryPlanner: Decided to Scan at res: $selectedRes, at rows: $rows, for BBox: ${rq.bbox}")
      // setup the RasterFilteringIterator
      val cfg = new IteratorSetting(RFI.priority, RFI.name, classOf[RFI])
      configureRasterFilter(cfg, constructRasterFilter(rq.bbox.geom, rasterSft))
      configureRasterMetadataFeatureType(cfg, rasterSft)

      // TODO: WCS: setup a CFPlanner to match against a list of strings
      // ticket is GEOMESA-559
      Some(BatchScanPlan(null, rows, Seq(cfg), Seq.empty[Text], null, -1, hasDuplicates = false))
    }
  }

  def selectResolution(suggestedResolution: Double, availableResolutions: List[Double]): Double = {
    logger.debug(s"RasterQueryPlanner: trying to get resolution $suggestedResolution " +
      s"from available Resolutions: ${availableResolutions.sorted}")
    val ret = if (availableResolutions.length <= 1) {
      availableResolutions.headOption.getOrElse(defaultResolution)
    } else {
      val finerResolutions = availableResolutions.filter(_ <= suggestedResolution)
      logger.debug(s"RasterQueryPlanner: Picking a resolution from: $finerResolutions")
      if (finerResolutions.isEmpty) availableResolutions.min else finerResolutions.max
    }
    logger.debug(s"RasterQueryPlanner: Decided to use resolution: $ret")
    ret
  }

  val ff = CommonFactoryFinder.getFilterFactory2

  def constructRasterFilter(geom: Geometry, featureType: SimpleFeatureType): Filter = {
    val property = ff.property(featureType.getGeometryDescriptor.getLocalName)
    val bounds = ff.literal(geom)
    // note: overlaps is not sufficient see DE-9IM definition
    ff.and(ff.intersects(property, bounds), ff.not(ff.touches(property, bounds)))
  }

  def configureRasterFilter(cfg: IteratorSetting, filter: Filter) =
    cfg.addOption(GEOMESA_ITERATORS_ECQL_FILTER, ECQL.toCQL(filter))

  def configureRasterMetadataFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SFT_NAME, rasterSftName)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  def modifyHashRange(hash: String, expectedLen: Int, res: String): ARange = expectedLen match {
    case 0                                     => new ARange(new Text(s"$res~"))
    case lucky if expectedLen == hash.length   => new ARange(new Text(s"$res~$hash"))
    case shorten if expectedLen < hash.length  => new ARange(new Text(s"$res~${hash.substring(0, expectedLen)}"))
    case lengthen if expectedLen > hash.length => new ARange(new Text(s"$res~$hash"), new Text(s"$res~$hash~"))
  }

}