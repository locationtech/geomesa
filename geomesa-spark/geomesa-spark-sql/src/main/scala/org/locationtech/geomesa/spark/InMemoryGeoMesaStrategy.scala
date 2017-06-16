package org.locationtech.geomesa.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

class InMemoryGeoMesaStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = ???
}
