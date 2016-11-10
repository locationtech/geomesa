/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.util.zip.GZIPOutputStream

import com.beust.jcommander.ParameterException
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{DataStore, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.tools.{BaseExportCommands, RootExportCommands}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait ExportCommandTools extends LazyLogging {

  def getFeatureCollection[P <: BaseExportCommands](overrideAttributes: Option[java.util.List[String]] = None,
                           ds: DataStore,
                           params: P): SimpleFeatureCollection = {
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    logger.debug(s"Applying CQL filter ${filter.toString}")
    val q = new Query(params.featureName, filter)
    Option(params.maxFeatures).foreach(q.setMaxFeatures(_))
    setOverrideAttributes(q, overrideAttributes.orElse(Option(seqAsJavaList(Seq(params.attributes)))))

    // get the feature store used to query the GeoMesa data
    val fs = ds.getFeatureSource(params.featureName)

    // and execute the query
    Try(fs.getFeatures(q)) match {
      case Success(fc) => fc
      case Failure(ex) =>
        throw new Exception("Error: Could not create a SimpleFeatureCollection to export. Please ensure " +
          "that all arguments are correct in the previous command.", ex)
    }
  }

  // If there are override attributes given as an arg or via command line params
  // split attributes by "," meanwhile allowing to escape it by "\,".
  def setOverrideAttributes(q: Query, overrideAttributes: Option[java.util.List[String]] = None) = {
    for ( list <- overrideAttributes;
          attributes: String <- asScalaBufferConverter(list).asScala.toSeq ){
      val splitAttrs = attributes.split("""(?<!\\),""").map(_.trim.replace("\\,", ","))
      logger.debug("Attributes used for query transform: " + splitAttrs.mkString("|"))
      q.setPropertyNames(splitAttrs)
    }
  }

  def createOutputStream[P <: RootExportCommands](skipCompression: Boolean = false, params: P): OutputStream = {
    val out = if (params.file == null) System.out else new FileOutputStream(params.file)
    val compressed = if (skipCompression || params.gzip == null) out else new GZIPOutputStream(out) {
      `def`.setLevel(params.gzip) // hack to access the protected deflate level
    }
    new BufferedOutputStream(compressed)
  }

  // noinspection AccessorLikeMethodIsEmptyParen
  def getWriter[P <: RootExportCommands](params: P): Writer = new OutputStreamWriter(createOutputStream(false, params))

  def checkShpFile[P <: RootExportCommands](params: P): File = {
    if (params.file != null) {
      params.file
    } else {
      throw new ParameterException("Error: -o or --output for file-based output is required for " +
        "shapefile export (stdout not supported for shape files)")
    }
  }
}

