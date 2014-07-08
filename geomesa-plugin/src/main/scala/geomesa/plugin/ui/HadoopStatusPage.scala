package geomesa.plugin.ui

import org.apache.hadoop.mapred.JobClient
import org.apache.wicket.markup.html.WebMarkupContainer
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.model.Model

import scala.util.{Failure, Success, Try}

/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    val jobClient = {
      val configuration = GeoMesaBasePage.getHdfsConfiguration
      new JobClient(configuration)
    }
    Try(jobClient.getClusterStatus) match {
      // TODO is there some way to tell if a cluster is valid or not? the status says 'running' even
      // if not actually connected to anything
      case Success(cluster) =>
        error.setVisible(false)

        val activeJobs = Try(jobClient.jobsToComplete.toList.map(_.getJobID.toString))
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
        errorMessageModel.setObject(s"Hadoop configuration error: ${e.getMessage}")
    }
  }

}
