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

function drawApplicationTimeline(eventObjArray) {
  var visDataSet = new vis.DataSet(eventObjArray);
  var options = {
    editable: false
  };

  var container = document.getElementById('application-timeline');
  var timeline = new vis.Timeline(container, visDataSet, options);
}

function drawJobTimeline(eventObjArray) {
  var visDataSet = new vis.DataSet(eventObjArray);
  var options = {
    editable: false
  };

  var container = document.getElementById('job-timeline');
  var timeline = new vis.Timeline(container, visDataSet, options);
}

var taskTimeline
function drawTaskAssignmentTimeline(groupArray, eventObjArray) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);

  var container = document.getElementById('task-assignment-timeline');
  var options = {
    groupOrder: function (a, b) {
      return a.value - b.value
    },
    editable: false,
    align: 'left',
    selectable: false,
    showCurrentTime: false,
    zoomable: false
  };

  taskTimeline = new vis.Timeline(container)
  taskTimeline.setOptions(options);
  taskTimeline.setGroups(groups);
  taskTimeline.setItems(items);

  $("#task-timeline-zoom-lock").click(function() {
    if (this.checked) {
      taskTimeline.setOptions({zoomable: false})
    } else {
      taskTimeline.setOptions({zoomable: true})
    }
  })

  $("div.item.range").map(function(idx, elem) {
    return elem.setAttribute("style", elem.getAttribute("style") + " padding: 0px 0px 0px 0px; height: 40px;"
      /* + " position: relative; height: 36px;"*/);
  });

  $("div.item.range>div").map(function(idx, elem) {
    return elem.setAttribute("style", elem.getAttribute("style") + " height: 100%; width: 100%;"/* +
      " position: absolute; height: 100%; width: 100%;"*/);
  });
}

/*
var data = new vis.DataSet([
          {
          jobEventList.map {
            case (jobId, startTime, endTime) =>
              s"""
                  |{
                  |  'start': new Date(${startTime}),
                  |  'end': new Date(${endTime}),
                  |  'content': 'Job ID=${jobId}'
                  |},
                """.stripMargin
          }
          }
          ]);
          {
          """
            |var options = {
            |  editable: false
            |}
          """.stripMargin
          }

          var container = document.getElementById('application-timeline');
          timeline = new vis.Timeline(container, data, options);

*/