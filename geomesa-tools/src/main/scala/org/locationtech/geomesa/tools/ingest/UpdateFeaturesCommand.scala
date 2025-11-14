/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.beust.jcommander.{IParameterValidator, IStringConverter, Parameter, ParameterException}
import org.apache.accumulo.access.{AccessExpression, IllegalAccessExpressionException}
import org.geotools.api.data.{DataStore, Transaction}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.ingest.UpdateFeaturesCommand.UpdateFeaturesParams
import org.locationtech.geomesa.tools.utils.{NoopParameterSplitter, Prompt}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.Flushable
import scala.util.Try

trait UpdateFeaturesCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  import scala.collection.JavaConverters._

  override val name = "update-features"
  override def params: UpdateFeaturesParams

  override def execute(): Unit = {
    val attributes = params.attributes.asScala.toMap
    val vis = Option(params.visibility).filterNot(_.isBlank)
    if (attributes.isEmpty && vis.isEmpty) {
      throw new ParameterException("Must specify at least one --set or --set-visibility")
    }
    withDataStore { ds =>
      val sft = Try(ds.getSchema(params.featureName)).getOrElse(null)
      if (sft == null) {
        throw new ParameterException(s"Schema '${params.featureName}' does not exist in the data store")
      } else if (attributes.keys.exists(a => sft.indexOf(a) == -1)) {
        val missing = attributes.keys.filter(a => sft.indexOf(a) == -1).mkString("'", "', '", "'")
        throw new ParameterException(
          s"Attribute(s) $missing do not exist in schema '${params.featureName}'. Available attributes: " +
            sft.getAttributeDescriptors.asScala.map(_.getLocalName).mkString(", "))
      }
      val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)
      val msg = new StringBuilder(s"Updating schema '${params.featureName}', setting ")
      if (attributes.nonEmpty) {
        msg.append(attributes.map { case (k, v) => s"'$k' to '$v'" }.mkString("attribute(s) ", ", ", ""))
        if (vis.nonEmpty) {
          msg.append(" and ")
        }
      }
      vis.foreach(v => msg.append(s"visibility to '$v'"))
      if (filter == Filter.INCLUDE) {
        msg.append(" for all features")
      } else {
        msg.append(s""" for features matching filter "${ECQL.toCQL(filter)}"""")
      }
      Command.user.info(msg.toString())
      if (params.force || Prompt.confirm("Continue (y/n)? ")) {
        Command.user.info(s"Updating features, please wait...")
        var count = 0L
        WithClose(ds.getFeatureWriter(params.featureName, filter, Transaction.AUTO_COMMIT)) { writer =>
          while (writer.hasNext) {
            val next = writer.next()
            attributes.foreach { case (k, v) => next.setAttribute(k, v) }
            vis.foreach(next.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, _))
            writer.write()
            count += 1
            if (count % 1000 == 0) {
              print('.')
              writer match {
                case f: Flushable => f.flush()
                case _ =>
              }
            }
          }
          println()
        }
        Command.user.info(s"Complete - updated $count total features")
      }
    }
  }
}

object UpdateFeaturesCommand {

  // @Parameters(commandDescription = "Update attributes or visibilities of features in GeoMesa.")
  trait UpdateFeaturesParams extends RequiredTypeNameParam with OptionalCqlFilterParam with OptionalForceParam {

    @Parameter(
      names = Array("--set"),
      description = "Name and value of an attribute to update, e.g. 'name=bob'",
      validateWith = Array(classOf[TupleValidator]),
      converter = classOf[TupleConverter],
      splitter = classOf[NoopParameterSplitter])
    var attributes: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()

    @Parameter(
      names = Array("--set-visibility"),
      description = "Visibility to set on updated features",
      validateWith = Array(classOf[VisibilityValidator]))
    var visibility: String = _
  }

  class TupleConverter extends IStringConverter[(String, String)] {
    override def convert(value: String): (String, String) = {
      value.split("=", 2) match {
        case Array(one, two) => (one, two)
      }
    }
  }

  class TupleValidator extends IParameterValidator {
    @throws[ParameterException]
    override def validate(name: String, value: String): Unit = {
      if (value == null || value.isEmpty || value.indexOf('=') == -1) {
        throw new ParameterException(s"Parameter $name $value is not a valid tuple of the form 'k=v'")
      }
    }
  }

  class VisibilityValidator extends IParameterValidator {
    @throws[ParameterException]
    override def validate(name: String, value: String): Unit = {
      try { AccessExpression.validate(value) } catch {
        case e: IllegalAccessExpressionException =>
          throw new ParameterException(s"Parameter $name $value is not a valid visibility: ${e.getMessage}", e)
      }
    }
  }
}
