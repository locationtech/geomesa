/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import com.beust.jcommander.{Parameter, ParameterException}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType

package object stats {

  import scala.collection.JavaConverters._

  // gets attributes to run stats on, based on sft and input params
  def getAttributesFromParams(sft: SimpleFeatureType, params: AttributeStatsParams): Seq[String] = {
    getAttributes(sft, params.attributes.asScala)
  }

  /**
    * Obtains attributes to run stats on
    *
    * @param sft the SFT to obtain attribute names from
    * @param attributes a list of attribute names to pull from the SFT
    * @return
    */
  def getAttributes(sft: SimpleFeatureType, attributes: Seq[String]): Seq[String] = {
    if (attributes.isEmpty) {
      sft.getAttributeDescriptors.asScala.filter(GeoMesaStats.okForStats).map(_.getLocalName)
    } else {
      val descriptors = attributes.map(sft.getDescriptor)
      if (descriptors.contains(null)) {
        val invalid = attributes.zip(descriptors).filter(_._2 == null).map(_._1) match {
          case Seq(a) => s"attribute '$a'"
          case seq => seq.mkString("attributes '", "', '", "'")
        }
        throw new ParameterException(s"Invalid $invalid for schema '${sft.getTypeName}'")
      }
      if (!descriptors.forall(GeoMesaStats.okForStats)) {
        val notOk = descriptors.filterNot(GeoMesaStats.okForStats)
        val invalid = notOk.map(d => s"${d.getLocalName}:${d.getType.getBinding.getSimpleName}") match {
          case Seq(a) => s"attribute '$a'"
          case seq => seq.mkString("attributes '", "', '", "'")
        }
        throw new ParameterException(s"Can't evaluate stats for $invalid due to unsupported data types")
      }
      attributes
    }
  }

  trait StatsParams extends RequiredTypeNameParam with OptionalCqlFilterParam {
    @Parameter(
      names = Array("--no-cache"),
      description = "Calculate against the data set instead of using cached statistics (may be slow)")
    var exact: Boolean = false
  }

  trait AttributeStatsParams {
    @Parameter(
      names = Array("-a", "--attributes"),
      description = "Attributes to evaluate (use multiple flags or separate with commas)")
    var attributes: java.util.List[String] = new java.util.ArrayList[String]()
  }
}
