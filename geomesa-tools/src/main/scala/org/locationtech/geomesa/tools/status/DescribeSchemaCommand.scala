/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.ParameterException
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools.{Command, DataStoreCommand, TypeNameParam}
import org.locationtech.geomesa.utils.stats.IndexCoverage

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

trait DescribeSchemaCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name: String = "describe-schema"
  override def params: TypeNameParam

  override def execute(): Unit = withDataStore(describe)

  protected def describe(ds: DS): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    import scala.collection.JavaConversions._

    Command.user.info(s"Describing attributes of feature '${params.featureName}'")

    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Feature '${params.featureName}' not found")
    }

    val namesAndDescriptions = sft.getAttributeDescriptors.map { descriptor =>
      val name = descriptor.getLocalName
      val description = ArrayBuffer.empty[String]
      if (descriptor == sft.getGeometryDescriptor) {
        description.append("(Spatially indexed)")
      } else if (sft.getDtgField.exists(_ == name)) {
        description.append("(Spatio-temporally indexed)")
      }
      descriptor.getIndexCoverage() match {
        case IndexCoverage.JOIN => description.append("(Attribute indexed - join)")
        case IndexCoverage.FULL => description.append("(Attribute indexed - full)")
        case _ => // no-op
      }
      Option(descriptor.getDefaultValue).foreach(v => description.append(s"Default Value: $v"))
      (name, descriptor.getType.getBinding.getSimpleName, description)
    }

    val maxName = namesAndDescriptions.map(_._1.length).max
    val maxType = namesAndDescriptions.map(_._2.length).max
    namesAndDescriptions.foreach { case (n, t, d) =>
      Command.output.info(s"${n.padTo(maxName, ' ')} | ${t.padTo(maxType, ' ')} ${d.mkString(" ")}")
    }

    val userData = sft.getUserData
    if (!userData.isEmpty) {
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.KEYWORDS_KEY
      Command.output.info("\nUser data:")
      val namesAndValues = userData.map { case (k, v) =>
        if (k == KEYWORDS_KEY) {
          (KEYWORDS_KEY, sft.getKeywords.mkString("[\"", "\", \"", "\"]"))
        } else {
          (s"$k", s"$v")
        }
      }
      val maxName = namesAndValues.map(_._1.length).max
      namesAndValues.toSeq.sortBy(_._1).foreach { case (n, v) =>
        Command.output.info(s"  ${n.padTo(maxName, ' ')} | $v")
      }
    }
  }
}
