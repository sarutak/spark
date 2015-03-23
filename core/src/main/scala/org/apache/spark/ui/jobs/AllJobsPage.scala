/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui.jobs

import scala.xml.{Node, NodeSeq, Unparsed}

import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.ui.jobs.UIData.JobUIData

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab) extends WebUIPage("") {
  private val startTime: Option[Long] = parent.sc.map(_.startTime)
  private val listener = parent.listener

  private def jobsTable(jobs: Seq[JobUIData]): Seq[Node] = {
    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)

    val columns: Seq[Node] = {
      <th>{if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"}</th>
      <th>Description</th>
      <th>Submitted</th>
      <th>Duration</th>
      <th class="sorttable_nosort">Stages: Succeeded/Total</th>
      <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
    }

    def makeRow(job: JobUIData): Seq[Node] = {
      val lastStageInfo = Option(job.stageIds)
        .filter(_.nonEmpty)
        .flatMap { ids => listener.stageIdToInfo.get(ids.max) }
      val lastStageData = lastStageInfo.flatMap { s =>
        listener.stageIdToData.get((s.stageId, s.attemptId))
      }

      val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
      val lastStageDescription = lastStageData.flatMap(_.description).getOrElse("")
      val duration: Option[Long] = {
        job.submissionTime.map { start =>
          val end = job.completionTime.getOrElse(System.currentTimeMillis())
          end - start
        }
      }
      val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
      val formattedSubmissionTime = job.submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
      val detailUrl =
        "%s/jobs/job?id=%s".format(UIUtils.prependBaseUri(parent.basePath), job.jobId)
      <tr>
        <td sorttable_customkey={job.jobId.toString}>
          {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
        </td>
        <td>
          <span class="description-input" title={lastStageDescription}>{lastStageDescription}</span>
          <a href={detailUrl}>{lastStageName}</a>
        </td>
        <td sorttable_customkey={job.submissionTime.getOrElse(-1).toString}>
          {formattedSubmissionTime}
        </td>
        <td sorttable_customkey={duration.getOrElse(-1).toString}>{formattedDuration}</td>
        <td class="stage-progress-cell">
          {job.completedStageIndices.size}/{job.stageIds.size - job.numSkippedStages}
          {if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)"}
          {if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)"}
        </td>
        <td class="progress-cell">
          {UIUtils.makeProgressBar(started = job.numActiveTasks, completed = job.numCompletedTasks,
           failed = job.numFailedTasks, skipped = job.numSkippedTasks,
           total = job.numTasks - job.numSkippedTasks)}
        </td>
      </tr>
    }

    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>{columns}</thead>
      <tbody>
        {jobs.map(makeRow)}
      </tbody>
    </table>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse.toSeq
      val failedJobs = listener.failedJobs.reverse.toSeq
      val now = System.currentTimeMillis

      val activeJobsTable =
        jobsTable(activeJobs.sortBy(_.submissionTime.getOrElse(-1L)).reverse)
      val completedJobsTable =
        jobsTable(completedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)
      val failedJobsTable =
        jobsTable(failedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)

      val shouldShowActiveJobs = activeJobs.nonEmpty
      val shouldShowCompletedJobs = completedJobs.nonEmpty
      val shouldShowFailedJobs = failedJobs.nonEmpty

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {if (startTime.isDefined) {
              // Total duration is not meaningful unless the UI is live
              <li>
                <strong>Total Duration: </strong>
                {UIUtils.formatDuration(now - startTime.get)}
              </li>
            }}
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            {
              if (shouldShowActiveJobs) {
                <li>
                  <a href="#active"><strong>Active Jobs:</strong></a>
                  {activeJobs.size}
                </li>
              }
            }
            {
              if (shouldShowCompletedJobs) {
                <li>
                  <a href="#completed"><strong>Completed Jobs:</strong></a>
                  {completedJobs.size}
                </li>
              }
            }
            {
              if (shouldShowFailedJobs) {
                <li>
                  <a href="#failed"><strong>Failed Jobs:</strong></a>
                  {failedJobs.size}
                </li>
              }
            }
          </ul>
        </div>

      val _6list = scala.collection.mutable.HashMap[(Int, Int), scala.collection.mutable.ListBuffer[UIData.TaskUIData]]()
/*      def _6sigma {
        completedJobs.foreach {
          case uiData =>
            uiData.stageIds.foreach {
              case stageId =>
                val stageInfo = listener.stageIdToInfo(stageId)
                val stageData = listener.stageIdToData.get(stageInfo.stageId, stageInfo.attemptId).get
                val tasks = stageData.taskData.values.toSeq.sortBy(_.taskInfo.launchTime)
                val avg: Double = tasks.foldLeft[Double](0)(_ + _.taskInfo.duration) / tasks.length
                val std: Double = math.sqrt(tasks.map(x => math.pow(x.taskInfo.duration - avg, 2)).reduce(_ + _) / tasks.length)
                tasks.foreach {
                  task =>
                    if (avg - 6 * std > task.taskInfo.duration || avg + 6 * std < task.taskInfo.duration) {
                      _6list.getOrElseUpdate((stageInfo.stageId, stageInfo.attemptId), new scala.collection.mutable.ListBuffer[UIData.TaskUIData]()).append(task)
                    }
                }
            }
        }
      }*/
//      _6sigma
      var content = summary

      val jobEventList = listener.completedJobs.map(x => (x.jobId, x.submissionTime.getOrElse(-1), x.completionTime.getOrElse(-1))) ++
        listener.failedJobs.map(x => (x.jobId, x.submissionTime.getOrElse(-1), x.completionTime.getOrElse(-1)))
      val executorAddedEventList = listener.executorIdToAddedTime.map(kv => (kv._1, kv._2))
      val executorRemovedEventList = listener.executorIdToRemovedTimeAndReason.map(kv => (kv._1, kv._2))
      val jobEventArray = jobEventList.map {
        case (jobId, submissionTime, completionTime) =>
          s"""
            |{
            |  'start': new Date(${submissionTime}),
            |  'end': new Date(${completionTime}),
            |  'content': 'Job ${jobId}'
            |}
          """.stripMargin
      }

      val executorAddedEventArray = executorAddedEventList.map {
        case (executorId, addedTime) =>
          s"""
            |{
            |  'start': new Date(${addedTime}),
            |  'content': 'Executor ${executorId} added'
            |}
          """.stripMargin
      }
      val executorRemovedEventArray = executorRemovedEventList.map {
        case (executorId, (removedTime, reason)) =>
          s"""
            |{
            |  'start': new Date(${removedTime}),
            |  'content': '<a data-toggle="tooltip" data-placement="auto" title="${reason}">Executor ${executorId} removed</a>'
            |}
          """.stripMargin
      }

      val eventArrayStr =
        (jobEventArray ++ executorAddedEventArray ++ executorRemovedEventArray).mkString("[", ",", "]")

      content ++= <h4>Events on Application Timeline</h4> ++ <div id="application-timeline"></div>
      content ++=
        <script type="text/javascript">
          {Unparsed(s"drawApplicationTimeline(${eventArrayStr});")}
        </script>

      if (shouldShowActiveJobs) {
        content ++= <h4 id="active">Active Jobs ({activeJobs.size})</h4> ++
          activeJobsTable
      }
      if (shouldShowCompletedJobs) {
        content ++= <h4 id="completed">Completed Jobs ({completedJobs.size})</h4> ++
          completedJobsTable
/*        if (_6list.nonEmpty) {
          content ++= <table>
            <tr>
              <th>Stage ID</th> <th>Attempt ID</th> <th>Task ID</th> <th>Task Attempt ID</th><th>Executor ID</th><th>Host</th>
            </tr>{_6list.keysIterator.flatMap {
              case (stageId, stageAttemptId) =>
                _6list((stageId, stageAttemptId)).map {
                  value =>
                    <tr>
                      <td>
                        {stageId}
                      </td> <td>
                      {stageAttemptId}
                    </td> <td>
                      {value.taskInfo.taskId}
                    </td><td>
                      {value.taskInfo.attempt}</td>
                      <td>{value.taskInfo.duration}</td>
                      <td>{value.taskInfo.executorId}</td>
                      <td>{value.taskInfo.host}</td>
                    </tr>
                }
            }}
          </table>
        }*/
      }
      if (shouldShowFailedJobs) {
        content ++= <h4 id ="failed">Failed Jobs ({failedJobs.size})</h4> ++
          failedJobsTable
      }

      val helpText = """A job is triggered by an action, like "count()" or "saveAsTextFile()".""" +
        " Click on a job's title to see information about the stages of tasks associated with" +
        " the job."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}
