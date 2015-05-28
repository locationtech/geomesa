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
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.accumulo.data.extractDtgField
import org.locationtech.geomesa.tools.commands.DescribeCommand._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.opengis.feature.`type`.AttributeDescriptor

import scala.collection.JavaConversions._

class DescribeCommand(parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "describe"
  override val params = new DescribeParameters

  def execute() = {
    logger.info(s"Describing attributes of feature '${params.featureName}' from catalog table '$catalog'...")
    try {
      val sft = ds.getSchema(params.featureName)

      def isIndexed(attr: AttributeDescriptor) = attr.isIndexed

      val sb = new StringBuilder()
      sft.getAttributeDescriptors.foreach { attr =>
        sb.clear()
        val name = attr.getLocalName

        // TypeName
        sb.append(name)
        sb.append(": ")
        sb.append(attr.getType.getBinding.getSimpleName)

        if (extractDtgField(sft) == name)      sb.append(" (ST-Time-index)")
        if (sft.getGeometryDescriptor == attr) sb.append(" (ST-Geo-index)")
        if (isIndexed(attr))                   sb.append(" (Indexed)")
        if (attr.getDefaultValue != null)      sb.append("- Default Value: ", attr.getDefaultValue)

        println(sb.toString())
      }
    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
    }
  }

}

object DescribeCommand {
  @Parameters(commandDescription = "Describe the attributes of a given feature in GeoMesa")
  class DescribeParameters extends FeatureParams {}
}

