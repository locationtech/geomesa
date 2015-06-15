package org.locationtech.geomesa.plugin.ui

import org.apache.hadoop.mapred.JobClient
import org.apache.wicket.markup.html.WebMarkupContainer
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.model.Model
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
class HadoopStatusPage extends GeoMesaBasePage {

  // models to hold string values that we want to be able to update later
  val jobTrackerStatusModel = Model.of("UNAVAILABLE")
  val nodesModel = Model.of("")
  val mapSlotsModel = Model.of("")
  val reduceSlotsModel = Model.of("")
  val activeJobsModel = Model.of("")
  val errorMessageModel = Model.of("")

  val jobTrackerStatus = new Label("jobTrackerStatus", jobTrackerStatusModel)
  val nodes = new Label("nodes", nodesModel)
  val mapSlots = new Label("mapSlots", mapSlotsModel)
  val reduceSlots = new Label("reduceSlots", reduceSlotsModel)
  val activeJobs = new Label("activeJobs", activeJobsModel)
  activeJobs.setEscapeModelStrings(false)

  // error label - will be set invisible if no error
  val error = new WebMarkupContainer("error")
  val errorMessage = new Label("errorMessage", errorMessageModel)

  add(error)
  error.add(errorMessage)

  add(jobTrackerStatus)
  add(nodes)
  add(mapSlots)
  add(reduceSlots)
  add(activeJobs)

  loadStatus()

  /**
   * Loads the HDFS status and populates the labels
   */
  private def loadStatus(): Unit = {
    val jobClient = Try(new JobClient(GeoMesaBasePage.getHdfsConfiguration))
    jobClient.flatMap(jc => Try(jc.getClusterStatus)) match {
      // TODO is there some way to tell if a cluster is valid or not? the status says 'running' even
      // if not actually connected to anything
      case Success(cluster) =>
        error.setVisible(false)

        val activeJobs = jobClient.flatMap(jc => Try(jc.jobsToComplete.toList.map(_.getJobID.toString)))
                           .getOrElse(List.empty)
        val activeJobsString = activeJobs.length match {
          case 0 => "none"
          // TODO should change this to prevent possible markup injection
          // use a wicket repeating widget instead of markup to put each job on its own line
          // see 'activeJobs' label also
          case _ => activeJobs.mkString("<br/>")
        }

        // TODO the number of mappers/reducers returns 1 even if it should be 0
        // see https://issues.apache.org/jira/browse/MAPREDUCE-4288
        jobTrackerStatusModel.setObject(cluster.getJobTrackerStatus.toString)
        nodesModel.setObject(s"${cluster.getTaskTrackers} / ${cluster.getBlacklistedTrackers}")
        mapSlotsModel.setObject(s"${cluster.getMapTasks} / ${cluster.getMaxMapTasks}")
        reduceSlotsModel.setObject(s"${cluster.getReduceTasks} / ${cluster.getMaxReduceTasks}")
        activeJobsModel.setObject(activeJobsString)
      case Failure(e) =>
        // we have to get a logger instance each time since the page has to be serializable
        LoggerFactory.getLogger(classOf[HadoopStatusPage]).error("Error retrieving hadoop status", e)
        errorMessageModel.setObject(s"Hadoop configuration error: ${e.getMessage}")
    }
  }

}
