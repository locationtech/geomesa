package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

case class GeoMesaCacheTableCommand(tableIdent: TableIdentifier,
                                     plan: Option[LogicalPlan],
                                     isLazy: Boolean) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {



    Seq.empty[Row]
  }
}
