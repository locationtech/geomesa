/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.data

import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.RasterTestsUtils._
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.locationtech.geomesa.utils.stats.{NoOpTimings, Timings}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloRasterStoreQueryIntegratedTest extends Specification {

  sequential

  var testIteration = 0

  def getNewIteration() = {
    testIteration += 1
    s"testRSQIT_Table_$testIteration"
  }

  def correctRes(r: Double): Double = BigDecimal(r).round(mc).toDouble

  "RasterStore" should {
    "create an empty table and be able delete itself" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      val tableOps = rasterStore.connector.tableOperations()

      tableOps.exists(tableName) must beTrue
      tableOps.exists(s"${tableName}_queries") must beTrue
      tableOps.exists(GEOMESA_RASTER_BOUNDS_TABLE) must beTrue

      rasterStore.deleteRasterTable()

      tableOps.exists(tableName) must beFalse
      tableOps.exists(s"${tableName}_queries") must beFalse
      // the deleteRasterTable does not delete the bounds table, just metadata rows contained in the table
      tableOps.exists(GEOMESA_RASTER_BOUNDS_TABLE) must beTrue
    }

    "create a table, write to it, and be able delete itself" in {
      val tableName = getNewIteration()
      val theStore = createMockRasterStore(tableName)

      val tableOps = theStore.connector.tableOperations()

      tableOps.exists(tableName) must beTrue
      tableOps.exists(s"${tableName}_queries") must beTrue
      tableOps.exists(GEOMESA_RASTER_BOUNDS_TABLE) must beTrue

      // populate store
      val testRaster = generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster)

      //test the query
      val query = generateQuery(0, 50, 0, 50)
      val rasters = theStore.getRasters(query).toList
      rasters.length must beEqualTo(1)

      // test the bounds
      theStore.getResToGeoHashLenMap.isEmpty must beFalse
      val theBounds = theStore.getBounds
      theBounds must beAnInstanceOf[BoundingBox]
      theBounds.maxLon must beEqualTo(50.0)
      theBounds.maxLat must beEqualTo(50.0)
      theBounds.minLon must beEqualTo(0.0)
      theBounds.minLat must beEqualTo(0.0)

      // Now delete the table and test if things worked
      theStore.deleteRasterTable()

      val nullBounds = theStore.getBounds
      nullBounds.maxLon must beEqualTo(180.0)
      nullBounds.maxLat must beEqualTo(90.0)
      nullBounds.minLon must beEqualTo(-180.0)
      nullBounds.minLat must beEqualTo(-90.0)

      tableOps.exists(tableName) must beFalse
      tableOps.exists(s"${tableName}_queries") must beFalse
      // the deleteRasterTable does not delete the bounds table, just metadata rows contained in the table
      tableOps.exists(GEOMESA_RASTER_BOUNDS_TABLE) must beTrue
      theStore.getResToGeoHashLenMap.isEmpty must beTrue
    }

    "create an empty RasterStore and return nothing" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query)
      theResults.toList.length must beEqualTo(0)
    }

    "create a Raster Store, populate it and run a query" in {
      val tableName = getNewIteration()
      val theStore = createMockRasterStore(tableName)

      // populate store
      val testRaster = generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      theStore must beAnInstanceOf[AccumuloRasterStore]
      val theIterator = theStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter in a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster = generateTestRasterFromGeoHash(GeoHash("s"))
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theIterator = rasterStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter out a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster = generateTestRasterFromGeoHash(GeoHash("d"))
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 45, 0, 45)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theIterator = rasterStore.getRasters(query)
      theIterator.isEmpty must beTrue
    }

    "Properly filter out a raster via a query bbox and maintain a valid raster in the results" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster1 = generateTestRasterFromGeoHash(GeoHash("s"))
      rasterStore.putRaster(testRaster1)
      val testRaster2 = generateTestRasterFromGeoHash(GeoHash("d"))
      rasterStore.putRaster(testRaster2)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly filter in a raster conforming to GeoHashes via a query bbox and resolution" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster = generateTestRasterFromGeoHash(GeoHash("s"), res = 5.0)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 50, 0, 50, res = 5.0)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a raster slightly smaller than a GeoHash via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val gh = GeoHash("dqcjr")
      val env = gh.getEnvelopeInternal
      val inG = gh.geom.buffer(-0.0001).getEnvelopeInternal
      val testRaster = generateTestRaster(inG.getMinX, inG.getMaxX, inG.getMinY, inG.getMaxY)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(env.getMinX-0.0001, env.getMaxX+0.0001, env.getMinY-0.0001, env.getMaxY+0.0001)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a raster slightly larger than a GeoHash via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val gh = GeoHash("dqcjr")
      val env = gh.getEnvelopeInternal
      val inG = gh.geom.buffer(0.0001).getEnvelopeInternal
      val testRaster = generateTestRaster(inG.getMinX, inG.getMaxX, inG.getMinY, inG.getMaxY)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(env.getMinX-0.0001, env.getMaxX+0.0001, env.getMinY-0.0001, env.getMaxY+0.0001)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }
    
    "Properly return a group of four Rasters Conforming to GeoHashes Near (0, 0)" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val bbox1 = GeoHash("7").bbox
      val testRaster1 = generateTestRasterFromBoundingBox(bbox1)
      rasterStore.putRaster(testRaster1)
      val bbox2 = GeoHash("k").bbox
      val testRaster2 = generateTestRasterFromBoundingBox(bbox2)
      rasterStore.putRaster(testRaster2)
      val bbox3 = GeoHash("s").bbox
      val testRaster3 = generateTestRasterFromBoundingBox(bbox3)
      rasterStore.putRaster(testRaster3)
      val bbox4 = GeoHash("e").bbox
      val testRaster4 = generateTestRasterFromBoundingBox(bbox4)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = generateQuery(bbox1.minLon, bbox3.maxLon, bbox1.minLat, bbox3.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(4)
    }

    "Properly return a group of four Small Rasters Conforming to GeoHashes" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val bbox1 = GeoHash("dqb0m").bbox
      val testRaster1 = generateTestRasterFromBoundingBox(bbox1)
      rasterStore.putRaster(testRaster1)
      val bbox2 = GeoHash("dqb0q").bbox
      val testRaster2 = generateTestRasterFromBoundingBox(bbox2)
      rasterStore.putRaster(testRaster2)
      val bbox3 = GeoHash("dqb0w").bbox
      val testRaster3 = generateTestRasterFromBoundingBox(bbox3)
      rasterStore.putRaster(testRaster3)
      val bbox4 = GeoHash("dqb0t").bbox
      val testRaster4 = generateTestRasterFromBoundingBox(bbox4)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = generateQuery(bbox1.minLon, bbox3.maxLon, bbox1.minLat, bbox3.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(4)
    }

    "Do the correct thing when querying the whole world" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val wholeWorld = BoundingBox(-180.0, 180, -90.0, 90.0)
      val allFiveCharacterHashes = BoundingBox.getGeoHashesFromBoundingBox(wholeWorld)
      val testRasters = allFiveCharacterHashes.map{ hash => generateTestRasterFromBoundingBox(GeoHash(hash).bbox) }
      testRasters.foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(-180.0, 180.0, -90.0, 90.0)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(32)
    }

    "Properly return one raster in a QLevel 1 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.quadrant1
      RasterTestsUtils.generateQuadTreeLevelRasters(1).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 2 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(2, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(2).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 3 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(3, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(3).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 4 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(4, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(4).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 5 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(5, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(5).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 6 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(6, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(6).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 7 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(7, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(7).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 8 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(8, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(8).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 9 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(9, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(9).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 10 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(10, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(10).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 11 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(11, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(11).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 12 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(12, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(12).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 13 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(13, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(13).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 14 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(14, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(14).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 15 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(15, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(15).foreach(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Given a simple pyramid, select the top level when doing a whole world query" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // expected res
      val expectedResolution = correctRes(50.0 / 256)

      // general setup
      val testRaster1 = generateTestRaster(0, 45.0, 0, 45.0, res = 50.0 / 256)
      rasterStore.putRaster(testRaster1)
      val testRaster2 = generateTestRaster(0, 45.0/2, 0, 45.0/2, res = 40.0 / 256)
      rasterStore.putRaster(testRaster2)
      val testRaster3 = generateTestRaster(0, 45.0/4, 0, 45.0/4, res = 30.0 / 256)
      rasterStore.putRaster(testRaster3)
      val testRaster4 = generateTestRaster(0, 45.0/8, 0, 45.0/8, res = 20.0 / 256)
      rasterStore.putRaster(testRaster4)
      val testRaster5 = generateTestRaster(0, 45.0/16, 0, 45.0/16, res = 10.0 / 256)
      rasterStore.putRaster(testRaster5)
      //generate query
      val query = generateQuery(-180.0, 180.0, -90.0, 90.0)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
      theResults.head.resolution must beEqualTo(expectedResolution)
    }

    "Given a odd pyramid (equal resolutions at varying GeoHash precision), return the correct Availability Map" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster1 = generateTestRaster(0, 45.0, 0, 45.0, res = 50.0 / 256)
      rasterStore.putRaster(testRaster1)
      val testRaster2 = generateTestRaster(0, 45.0/2, 0, 45.0/2, res = 40.0 / 256)
      rasterStore.putRaster(testRaster2)
      val testRaster3 = generateTestRaster(0, 45.0/4, 0, 45.0/4, res = 50.0 / 256)
      rasterStore.putRaster(testRaster3)
      val testRaster4 = generateTestRaster(0, 45.0/8, 0, 45.0/8, res = 50.0 / 256)
      rasterStore.putRaster(testRaster4)

      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val theAvailability = rasterStore.getResToGeoHashLenMap
      theAvailability.keys().size() must beEqualTo(3)
      theAvailability.keySet().size() must beEqualTo(2)
      theAvailability.values().size() must beEqualTo(3)
    }

  }
}
