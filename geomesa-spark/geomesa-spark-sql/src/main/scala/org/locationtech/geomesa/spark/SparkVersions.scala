/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation

import scala.util.Try

/**
  * Utility to smooth over differences in spark API versions using reflection
  */
object SparkVersions {

  private val _copyLogicalRelation: Try[(LogicalRelation, BaseRelation, Seq[AttributeReference]) => LogicalRelation] = Try {
    val methods = classOf[LogicalRelation].getMethods
    val m = methods.find(m => m.getName == "copy" && Seq(3, 4).contains(m.getParameterCount)).getOrElse {
      throw new NoSuchMethodError(s"Could not find method named 'copy' in class ${classOf[LogicalRelation].getName}")
    }
    if (m.getParameterCount == 4) {
      val streaming = methods.find(_.getName == "isStreaming").getOrElse {
        throw new NoSuchMethodError("Could not find method named 'isStreaming' in class " +
            classOf[LogicalRelation].getName)
      }
      (r, b, o) => m.invoke(r, b, o, r.catalogTable, streaming.invoke(r)).asInstanceOf[LogicalRelation]
    } else {
      (r, b, o) => m.invoke(r, b, o, r.catalogTable).asInstanceOf[LogicalRelation]
    }
  }

  private val _copyJoin: Try[(Join, LogicalPlan, LogicalPlan, JoinType, Option[Expression]) => Join] = Try {
    val methods = classOf[Join].getMethods
    val m = methods.find(m => m.getName == "copy" && Seq(4, 5).contains(m.getParameterCount)).getOrElse {
      throw new NoSuchMethodError(s"Could not find method named 'copy' in class ${classOf[Join].getName}")
    }
    if (m.getParameterCount == 5) {
      val hint = methods.find(_.getName == "hint").getOrElse {
        throw new NoSuchMethodError(s"Could not find method named 'hint' in class ${classOf[Join].getName}")
      }
      (j, l, r, t, c) => m.invoke(j, l, r, t, c, hint.invoke(j)).asInstanceOf[Join]
    } else {
      (j, l, r, t, c) => m.invoke(j, l, r, t, c).asInstanceOf[Join]
    }
  }

  /**
   * Value class to avoid runtime allocation
   *
   * @param r
   */
  class CopyLogicalRelation(val r: LogicalRelation) extends AnyVal {

    /**
     * Copy the relation
     * @param relation base relation
     * @param output output
     * @return
     */
    def apply(relation: BaseRelation = r.relation, output: Seq[AttributeReference] = r.output): LogicalRelation =
      _copyLogicalRelation.get.apply(r, relation, output)
  }

  /**
   * Value class to avoid runtime allocation
   *
   * @param j
   */
  class CopyJoin(val j: Join) extends AnyVal {

    /**
     * Copy the join
     *
     * @param left left plan
     * @param right right plan
     * @param joinType join type
     * @param condition join condition
     * @return
     */
    def apply(
        left: LogicalPlan = j.left,
        right: LogicalPlan = j.right,
        joinType: JoinType = j.joinType,
        condition: Option[Expression] = j.condition): Join =
      _copyJoin.get.apply(j, left, right, joinType, condition)
  }

  /**
    * Replacement for LogicalRelation#copy
    *
    * @param r relation to copy

    * @return
    */
  def copy(r: LogicalRelation): CopyLogicalRelation = new CopyLogicalRelation(r)

  /**
   * Replacement for Join#copy
   *
   * @param j join
   * @return
   */
  def copy(j: Join): CopyJoin = new CopyJoin(j)
}
