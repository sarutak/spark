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

  setupZoomable("#task-timeline-zoom-lock", taskTimeline)

  $.each($(".task-assignment-timeline-duration-bar>rect"), function(idx, elem) {
    elem.setAttribute("height", "100%");
    elem.setAttribute("y", "0");
  })

  $.each($("rect.task-status-legend"), function(idx, elem) {
    elem.setAttribute("rx", "2px");
    elem.setAttribute("stroke", "#97B0F8");

    var fillColor

    /**
     * JQuery doesn't support hasClass method for SVG
     * so we should use getAttribute for now.
     */
    var classInElem = elem.getAttribute("class");
    if (classInElem.indexOf(" succeeded") >= 0) {
      fillColor = "#D5DDF6"
    } else if (classInElem.indexOf(" failed") >= 0) {
      fillColor = "#FF5475"
    } else {
      fillColor = "#E3AAD6"
    }
    elem.setAttribute("fill", fillColor);
    elem.setAttribute("width", "20px");
    elem.setAttribute("height", "15px");
    elem.setAttribute("y", "5px");
  });

  $("rect.task-status-legend")
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
