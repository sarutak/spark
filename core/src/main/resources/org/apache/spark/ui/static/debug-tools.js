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

function drawApplicationTimeline(groupArray, eventObjArray) {
var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $("#application-timeline")[0];
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value
    },
    editable: false,
    selectable: false,
    showCurrentTime: false,
    zoomable: false
  };

  //var container = document.getElementById('application-timeline');

  //var timeline = new vis.Timeline(container, items, options);
  var applicationTimeline = new vis.Timeline(container);
  applicationTimeline.setOptions(options);
  applicationTimeline.setGroups(groups);
  applicationTimeline.setItems(items);
}

function drawJobTimeline(eventObjArray) {
  var visDataSet = new vis.DataSet(eventObjArray);
  var options = {
    editable: false,
    align: 'left',
    selectable: false,
    showCurrentTime: false,
    zoomable: false,
  };

  var container = document.getElementById('job-timeline');
  var timeline = new vis.Timeline(container, visDataSet, options);
}

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

  var taskTimeline = new vis.Timeline(container)
  taskTimeline.setOptions(options);
  taskTimeline.setGroups(groups);
  taskTimeline.setItems(items);

  setupZoomable('#task-timeline-zoom-lock', taskTimeline)
}

function setupZoomable(id, timeline) {
  $(id).click(function() {
    if (this.checked) {
      timeline.setOptions({zoomable: false});
    } else {
      timeline.setOptions({zoomable: true});
    }
  })
}
