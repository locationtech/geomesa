/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.geotools.coverage.grid.GridCoverage2D
import org.geoserver.catalog.Catalog
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.DirectPosition2D
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.joda.time.DateTime
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.coverage.PointOutsideCoverageException
import org.opengis.coverage.grid.GridCoverageReader

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

@DescribeProcess(
  title = "Raster Enrich Process",
  description = "Enriches Feature Collection from Raster layer"
)
class RasterEnrichProcess(catalog: Catalog) extends GeoMesaProcess {
  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Output collection")
  def execute(@DescribeParameter(name = "data", description = "Input features")
              obsFeatures: SimpleFeatureCollection,
              @DescribeParameter(name = "dateAttribute", description = "The date attribute")
              dateAttribute: String,
//              @DescribeParameter(name = "geomField", description = "The geom attribute")
//              geomField: String,
              @DescribeParameter(name = "coverage", description = "Input Coverage (ws1:layer1)")
              coverageName: String): SimpleFeatureCollection = {

    val split = coverageName.split(":")
    
    val reader: GridCoverageReader = catalog.getCoverageStoreByName(split(0), split(1)).getGridCoverageReader(null, null)
    val evaluator = new CoverageEvaluator(reader)

    val origSFT = obsFeatures.getSchema
    val encodedOrigSFT = SimpleFeatureTypes.encodeType(origSFT)

//    val specFrag = "wind_speed:Double,wind_direction:Double"
    val specFrag = evaluator.coverageNames.map( s => s.concat(":Double")).mkString(",")

    val sft = SimpleFeatureTypes.createType(origSFT.getName.getURI, s"$encodedOrigSFT,$specFrag")

    val builder = new SimpleFeatureBuilder(sft)

    val enrichedFeatures = CloseableIterator(obsFeatures.features()).map { sf =>
      builder.reset()
      builder.addAll(sf.getAttributes)

      val pt = sf.getDefaultGeometry.asInstanceOf[Point]
      val dt = sf.getAttribute(dateAttribute).asInstanceOf[Date]
      val ret: Seq[(Int, Double)] = evaluator.evalAtPoint(pt.getX, pt.getY, dt, sft)

      for  {
        (index, value) <- ret
      } {
        builder.set(index, value)
      }

      builder.buildFeature(sf.getID)
    }

    new ListFeatureCollection(sft, enrichedFeatures.toList)
  }
}
