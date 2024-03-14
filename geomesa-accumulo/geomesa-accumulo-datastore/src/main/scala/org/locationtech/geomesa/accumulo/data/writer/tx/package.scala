/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer

import org.apache.accumulo.core.data.{Condition, ConditionalMutation}
import org.apache.accumulo.core.security.ColumnVisibility
import org.locationtech.geomesa.utils.index.ByteArrays

import java.nio.charset.StandardCharsets

package object tx {

  sealed trait MutationBuilder {

    /**
     * Type of mutation, used for reporting errors
     *
     * @return
     */
    def name: String

    /**
     * Row being mutated
     *
     * @return
     */
    def row: Array[Byte]

    /**
     * Mutations
     *
     * @return
     */
    def kvs: Seq[MutationValue]

    /**
     * Create the mutation
     *
     * @return
     */
    def apply(): ConditionalMutation

    /**
     * Create a mutation that will undo the regular mutation from this object
     *
     * @return
     */
    def invert(): ConditionalMutation
  }

  object MutationBuilder {

    case class AppendBuilder(row: Array[Byte], kvs: Seq[MutationValue]) extends MutationBuilder {
      override def name: String = "insert"
      override def apply(): ConditionalMutation = {
        val mutation = new ConditionalMutation(row)
        kvs.foreach { kv =>
          mutation.addCondition(new Condition(kv.cf, kv.cq)) // requires cf+cq to not exist
          mutation.put(kv.cf, kv.cq, kv.vis, kv.value)
        }
        mutation
      }
      override def invert(): ConditionalMutation = DeleteBuilder(row, kvs)()
    }

    case class DeleteBuilder(row: Array[Byte], kvs: Seq[MutationValue]) extends MutationBuilder {
      override def name: String = "delete"
      override def apply(): ConditionalMutation = {
        val mutation = new ConditionalMutation(row)
        kvs.foreach { kv =>
          mutation.addCondition(new Condition(kv.cf, kv.cq).setVisibility(kv.vis).setValue(kv.value))
          mutation.putDelete(kv.cf, kv.cq, kv.vis)
        }
        mutation
      }
      override def invert(): ConditionalMutation = AppendBuilder(row, kvs)()
    }

    case class UpdateBuilder(row: Array[Byte], kvs: Seq[MutationValue], previous: Seq[MutationValue])
        extends MutationBuilder {
      override def name: String = "update"
      override def apply(): ConditionalMutation = {
        val mutation = new ConditionalMutation(row)
        previous.foreach(kv => mutation.addCondition(new Condition(kv.cf, kv.cq).setVisibility(kv.vis).setValue(kv.value)))
        kvs.foreach(kv => mutation.put(kv.cf, kv.cq, kv.vis, kv.value))
        mutation
      }
      override def invert(): ConditionalMutation = UpdateBuilder(row, previous, kvs)()
    }
  }

  /**
   * Holder for a mutation's values - i.e. the non-row parts of a key plus the value
   *
   * @param cf column family
   * @param cq column qualifier
   * @param vis visibility label
   * @param value value
   */
  case class MutationValue(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]) {
    def equalKey(other: MutationValue): Boolean = {
      java.util.Arrays.equals(cf, other.cf) &&
          java.util.Arrays.equals(cq, other.cq) &&
          java.util.Arrays.equals(vis.getExpression, other.vis.getExpression)
    }
    override def toString: String =
      s"cf: ${new String(cf, StandardCharsets.UTF_8)}, cq: ${new String(cq, StandardCharsets.UTF_8)}, " +
          s"vis: $vis, value: ${ByteArrays.toHex(value)}"
  }
}
