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
package org.locationtech.geomesa.tools.ingest

import java.awt.RenderingHints
import java.io.{File, Serializable}
import java.util.{Map => JMap}
import javax.media.jai.{JAI, ImageLayout}
import com.twitter.scalding.Args
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader
import org.geotools.coverageio.gdal.dted.DTEDReader
import org.geotools.factory.Hints
import org.geotools.gce.geotiff.GeoTiffReader
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.Utils.Formats._

import scala.collection.JavaConversions._
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.{params => dsp}

trait RasterIngest extends Logging {
  def getAccumuloCoverageStoreConf(config: Map[String, Option[String]]): JMap[String, Serializable] =
    mapAsJavaMap(Map(
      dsp.instanceIdParam.getName   -> config(IngestRasterParams.ACCUMULO_INSTANCE).get,
      dsp.zookeepersParam.getName   -> config(IngestRasterParams.ZOOKEEPERS).get,
      dsp.userParam.getName         -> config(IngestRasterParams.ACCUMULO_USER).get,
      dsp.passwordParam.getName     -> config(IngestRasterParams.ACCUMULO_PASSWORD).get,
      dsp.tableNameParam.getName    -> config(IngestRasterParams.TABLE).get,
      dsp.geoserverParam.getName    -> config(IngestRasterParams.GEOSERVER_REG),
      dsp.authsParam.getName        -> config(IngestRasterParams.AUTHORIZATIONS),
      dsp.visibilityParam.getName   -> config(IngestRasterParams.VISIBILITIES),
      dsp.shardsParam.getName       -> config(IngestRasterParams.SHARDS),
      dsp.writeMemoryParam.getName  -> config(IngestRasterParams.WRITE_MEMORY),
      dsp.writeThreadsParam         -> config(IngestRasterParams.WRITE_THREADS),
      dsp.queryThreadsParam.getName -> config(IngestRasterParams.QUERY_THREADS),
      dsp.mockParam.getName         -> config(IngestRasterParams.ACCUMULO_MOCK)
    ).collect {
      case (key, Some(value)) => (key, value);
      case (key, value: String) => (key, value)
    }).asInstanceOf[java.util.Map[String, Serializable]]

  def createCoverageStore(config: Map[String, Option[String]]): AccumuloCoverageStore = {
    val rasterName = config(IngestRasterParams.TABLE)
    if (rasterName == null || rasterName.isEmpty) {
      logger.error("No raster name specified for raster feature ingest." +
        " Please check that all arguments are correct in the previous command. ")
      sys.exit()
    }

    val csConfig: JMap[String, Serializable] = getAccumuloCoverageStoreConf(config)

    AccumuloCoverageStore(csConfig)
  }

  def createCoverageStore(args: Args): AccumuloCoverageStore = {
    val dsConfig: Map[String, Option[String]] =
      Map(
        IngestRasterParams.ZOOKEEPERS        -> args.optional(IngestRasterParams.ZOOKEEPERS),
        IngestRasterParams.ACCUMULO_INSTANCE -> args.optional(IngestRasterParams.ACCUMULO_INSTANCE),
        IngestRasterParams.ACCUMULO_USER     -> args.optional(IngestRasterParams.ACCUMULO_USER),
        IngestRasterParams.ACCUMULO_PASSWORD -> args.optional(IngestRasterParams.ACCUMULO_PASSWORD),
        IngestRasterParams.TABLE             -> args.optional(IngestRasterParams.TABLE),
        IngestRasterParams.GEOSERVER_REG     -> args.optional(IngestRasterParams.GEOSERVER_REG),
        IngestRasterParams.AUTHORIZATIONS    -> args.optional(IngestRasterParams.AUTHORIZATIONS),
        IngestRasterParams.VISIBILITIES      -> args.optional(IngestRasterParams.VISIBILITIES),
        IngestRasterParams.SHARDS            -> args.optional(IngestRasterParams.SHARDS),
        IngestRasterParams.WRITE_MEMORY      -> args.optional(IngestRasterParams.WRITE_MEMORY),
        IngestRasterParams.WRITE_THREADS     -> args.optional(IngestRasterParams.WRITE_THREADS),
        IngestRasterParams.QUERY_THREADS     -> args.optional(IngestRasterParams.QUERY_THREADS),
        IngestRasterParams.ACCUMULO_MOCK     -> args.optional(IngestRasterParams.ACCUMULO_MOCK)
      )
    createCoverageStore(dsConfig)
  }

  def getReader(imageFile: File, imageType: String): AbstractGridCoverage2DReader = {
    imageType match {
      case TIFF => getTiffReader(imageFile)
      case DTED => getDtedReader(imageFile)
      case _ => throw new Exception("Image type is not supported.")
    }
  }

  def getTiffReader(imageFile: File): AbstractGridCoverage2DReader = {
    new GeoTiffReader(imageFile, new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true))
  }

  def getDtedReader(imageFile: File): AbstractGridCoverage2DReader = {
    val l = new ImageLayout()
    l.setTileGridXOffset(0).setTileGridYOffset(0).setTileHeight(512).setTileWidth(512)
    val hints = new Hints
    hints.add(new RenderingHints(JAI.KEY_IMAGE_LAYOUT, l))
    new DTEDReader(imageFile, hints)
  }
}
