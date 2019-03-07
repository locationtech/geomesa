/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.shp

import java.nio.charset.StandardCharsets

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.shapefile.ShapefileDataStore
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.SimpleFeatureValidator
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory._
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

class ShapefileConverterFactory
  extends AbstractConverterFactory[ShapefileConverter, BasicConfig, BasicField, BasicOptions] {

  override protected val typeToProcess: String = ShapefileConverterFactory.TypeToProcess

  override protected implicit def configConvert: ConverterConfigConvert[BasicConfig] = BasicConfigConvert

  override protected implicit def fieldConvert: FieldConvert[BasicField] = BasicFieldConvert

  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert
}

object ShapefileConverterFactory extends LazyLogging {

  import org.locationtech.geomesa.utils.conversions.ScalaImplicits._

  import scala.collection.JavaConverters._

  val TypeToProcess: String = "shp"

  /**
    * Inferring a shapefile converter requires the path, so can't use the normal API method in
    * SimpleFeatureConverterFactory
    *
    * @param path path to a .shp file
    * @param sft simple feature type being used, if any
    * @return
    */
  def infer(path: String, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    var ds: ShapefileDataStore = null
    try {
      ds = ShapefileConverter.getDataStore(path)
      val fields = sft match {
        case None =>
          ds.getSchema.getAttributeDescriptors.asScala.mapWithIndex { case (d, i) =>
            BasicField(d.getLocalName, Some(Column(i + 1)))
          }

        case Some(s) =>
          // map the attributes whose name match the shapefile
          val descriptors = ds.getSchema.getAttributeDescriptors.asScala
          s.getAttributeDescriptors.asScala.flatMap { d =>
            val name = d.getLocalName
            var i = descriptors.indexWhere(_.getLocalName.equalsIgnoreCase(name))
            if (i == -1) { Seq.empty } else {
              Seq(BasicField(name, Some(Column(i + 1))))
            }
          }
      }

      val shpConfig = BasicConfig(TypeToProcess, Some(Column(0)), Map.empty, Map.empty)
      val options =
        BasicOptions(SimpleFeatureValidator.default, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)

      val config = BasicConfigConvert.to(shpConfig)
          .withFallback(BasicFieldConvert.to(fields))
          .withFallback(BasicOptionsConvert.to(options))
          .toConfig

      Some((sft.getOrElse(ds.getSchema), config))
    } catch {
      case NonFatal(e) =>
        logger.debug(s"Could not infer Shapefile converter from path '$path':", e)
        None
    } finally {
      if (ds != null) {
        ds.dispose()
      }
    }
  }
}
