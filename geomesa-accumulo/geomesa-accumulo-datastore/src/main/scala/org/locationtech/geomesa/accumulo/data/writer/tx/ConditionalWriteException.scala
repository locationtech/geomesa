/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer.tx

import org.apache.accumulo.core.client.ConditionalWriter
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException.ConditionalWriteStatus
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter.WriteException

/**
 * Exception for conditional write failures
 *
 * @param fid feature id
 * @param rejections failed mutations
 * @param msg exception message
 */
class ConditionalWriteException(fid: String, rejections: java.util.List[ConditionalWriteStatus], msg: String)
    extends WriteException(msg) {
  def getFeatureId: String = fid
  def getRejections: java.util.List[ConditionalWriteStatus] = rejections
}

object ConditionalWriteException {

  import scala.collection.JavaConverters._

  def apply(fid: String, rejections: Seq[ConditionalWriteStatus]): ConditionalWriteException = {
    new ConditionalWriteException(fid, rejections.asJava,
      s"Conditional write was rejected for feature '$fid': ${rejections.mkString(", ")}")
  }

  case class ConditionalWriteStatus(index: String, action: String, condition: ConditionalWriter.Status) {
    override def toString: String = s"$index $action $condition"
  }

  object ConditionalWriteStatus {
    def apply(index: GeoMesaFeatureIndex[_, _], action: String, condition: ConditionalWriter.Status): ConditionalWriteStatus = {
      val name = if (index.attributes.isEmpty) { index.name } else { s"${index.name}:${index.attributes.mkString(":")}" }
      ConditionalWriteStatus(name, action, condition)
    }
  }
}
