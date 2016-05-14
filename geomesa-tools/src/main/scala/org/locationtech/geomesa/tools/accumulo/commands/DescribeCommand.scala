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
import org.locationtech.geomesa.tools.accumulo.commands.DescribeCommand._
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class DescribeCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "describe"
  override val params = new DescribeParameters

  def execute() = {
    logger.info(s"Describing attributes of feature '${params.featureName}' from catalog table '$catalog'...")
    try {
      val sft = ds.getSchema(params.featureName)

      val sb = new StringBuilder()
      sft.getAttributeDescriptors.foreach { attr =>
        sb.clear()
        val name = attr.getLocalName

        // TypeName
        sb.append(name)
        sb.append(": ")
        sb.append(attr.getType.getBinding.getSimpleName)

        if (sft.getDtgField.exists(_ == name)) sb.append(" (ST-Time-index)")
        if (sft.getGeometryDescriptor == attr) sb.append(" (ST-Geo-index)")
        if (attr.isIndexed)                    sb.append(" (Indexed)")
        if (attr.getDefaultValue != null)      sb.append("- Default Value: ", attr.getDefaultValue)

        println(sb.toString())
      }

      val userData = sft.getUserData
      if (!userData.isEmpty) {
        println("\nUser data:")
        userData.foreach { case (key, value) => println(s"  $key: $value") }
      }

    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
      case NonFatal(e) =>
        logger.warn(s"Non fatal error encountered describing feature '${params.featureName}': ", e)
    }
  }

}

object DescribeCommand {
  @Parameters(commandDescription = "Describe the attributes of a given feature in GeoMesa")
  class DescribeParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam {}
}

