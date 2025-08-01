/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.shp

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.shp.ShapefileConverterFactory.TypeToProcess
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory._
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.locationtech.geomesa.utils.io.WithClose

import java.io.InputStream
import java.nio.charset.Charset
import scala.util.{Failure, Try}

class ShapefileConverterFactory
  extends AbstractConverterFactory[ShapefileConverter, BasicConfig, BasicField, BasicOptions](
    ShapefileConverterFactory.TypeToProcess, BasicConfigConvert, BasicFieldConvert, BasicOptionsConvert) {

  import scala.collection.JavaConverters._

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] =
    infer(is, sft, path.fold(Map.empty[String, AnyRef])(EvaluationContext.inputFileParam)).toOption

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {
    val url = hints.get(EvaluationContext.InputFilePathKey) match {
      case Some(p) => p.toString
      case None => return Failure(new RuntimeException("No file path specified to the input data"))

    }
    Try {
      WithClose(ShapefileConverter.getDataStore(url, ShapefileConverterFactory.DefaultCharset)) { ds =>
        val fields = sft match {
          case None =>
            var i = 0
            ds.getSchema.getAttributeDescriptors.asScala.map { d =>
              i += 1
              BasicField(d.getLocalName, Some(Column(i)))
            }

          case Some(s) =>
            // map the attributes whose name match the shapefile
            val descriptors = ds.getSchema.getAttributeDescriptors.asScala
            s.getAttributeDescriptors.asScala.flatMap { d =>
              val name = d.getLocalName
              val i = descriptors.indexWhere(_.getLocalName.equalsIgnoreCase(name))
              if (i == -1) { Seq.empty } else {
                Seq(BasicField(name, Some(Column(i + 1))))
              }
            }
        }

        val shpConfig = BasicConfig(TypeToProcess, None, Some(Column(0)), Map.empty, Map.empty)

        val config = BasicConfigConvert.to(shpConfig)
            .withFallback(BasicFieldConvert.to(fields.toSeq))
            .withFallback(BasicOptionsConvert.to(BasicOptions.default))
            .toConfig

        (sft.getOrElse(ds.getSchema), config)
      }
    }
  }

  override protected def withDefaults(conf: Config): Config =
    super.withDefaults(conf.withFallback(ShapefileConverterFactory.ShpConfigDefaults))
}

object ShapefileConverterFactory extends LazyLogging {

  val TypeToProcess: String = "shp"

  private val DefaultCharset: Charset = ShapefileDataStoreFactory.DBFCHARSET.getDefaultValue.asInstanceOf[Charset]

  private val ShpConfigDefaults: Config =
    ConfigFactory.empty().withValue("options.encoding", ConfigValueFactory.fromAnyRef(DefaultCharset.name()))
}
