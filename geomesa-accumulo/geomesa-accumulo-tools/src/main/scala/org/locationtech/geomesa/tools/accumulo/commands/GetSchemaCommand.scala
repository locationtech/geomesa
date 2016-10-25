/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.GetSchemaCommand._
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.geomesa.utils.stats.IndexCoverage

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class GetSchemaCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "get-schema"
  override val params = new DescribeParameters

  def execute() = {
    logger.info(s"Describing attributes of feature '${params.featureName}' from catalog table '$catalog'...")
    try {
      val sft = ds.getSchema(params.featureName)

      val sb = new StringBuilder()
      sft.getAttributeDescriptors.foreach { attr =>
        sb.clear()
        val name = attr.getLocalName
        val indexCoverage = attr.getIndexCoverage()

        // TypeName
        sb.append(name)
        sb.append(": ")
        sb.append(attr.getType.getBinding.getSimpleName)

        if (sft.getDtgField.exists(_ == name))        sb.append(" (ST-Time-index)")
        if (sft.getGeometryDescriptor == attr)        sb.append(" (ST-Geo-index)")
        if (indexCoverage == IndexCoverage.JOIN)      sb.append(" (Indexed - Join)")
        else if (indexCoverage == IndexCoverage.FULL) sb.append(" (Indexed - Full)")
        if (attr.getDefaultValue != null)             sb.append("- Default Value: ", attr.getDefaultValue)

        println(sb.toString())
      }

      val userData = sft.getUserData
      if (!userData.isEmpty) {
        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.KEYWORDS_KEY
        println("\nUser data:")
        val keywords = sft.getKeywords
        if (keywords.nonEmpty) {
          println(s"  $KEYWORDS_KEY: [${keywords.map("\"%s\"".format(_)).mkString(",")}]")
        }
        userData.filterKeys(_ != KEYWORDS_KEY).foreach {
          case (key, value) => println(s"  $key: $value")
        }
      }

    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
      case NonFatal(e) =>
        logger.warn(s"Non fatal error encountered describing feature '${params.featureName}': ", e)
    } finally {
      ds.dispose()
    }
  }

}

object GetSchemaCommand {
  @Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
  class DescribeParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam {}
}

