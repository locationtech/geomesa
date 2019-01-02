/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation

import scala.util.Try

/**
  * Utility to smooth over differences in spark API versions using reflection
  */
object SparkVersions {

  private val _copy: Try[(LogicalRelation, BaseRelation, Seq[AttributeReference]) => LogicalRelation] = Try {
    val methods = classOf[LogicalRelation].getMethods
    val m = methods.find(m => m.getName == "copy" && Seq(3, 4).contains(m.getParameterCount)).getOrElse {
      throw new NoSuchMethodError(s"Could not find method named 'copy' in class ${classOf[LogicalRelation].getName}")
    }
    if (m.getParameterCount == 4) {
      val streaming = methods.find(_.getName == "isStreaming").getOrElse {
        throw new NoSuchMethodError(s"Could not find method named 'isStreaming' in class " +
            classOf[LogicalRelation].getName)
      }
      (r, b, o) => m.invoke(r, b, o, r.catalogTable, streaming.invoke(r)).asInstanceOf[LogicalRelation]
    } else {
      (r, b, o) => m.invoke(r, b, o, r.catalogTable).asInstanceOf[LogicalRelation]
    }
  }

  /**
    * Replacement for LogicalRelation#copy
    *
    * @param r relation to copy
    * @param relation base relation
    * @param output output
    * @return
    */
  def copy(r: LogicalRelation)
          (relation: BaseRelation = r.relation,
           output: Seq[AttributeReference] = r.output): LogicalRelation = _copy.get.apply(r, relation, output)
}
