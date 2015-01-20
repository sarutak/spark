
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

function drawTaskAssignmentTimeline(groupArray, eventObjArray) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);

  var container = document.getElementById('task-assignment-timeline');
  var options = {
    groupOrder: function (a, b) {
      return a.value - b.value
    },
    editable: false
  };

  var timeline = new vis.Timeline(container)
  timeline.setOptions(options);
  timeline.setGroups(groups);
  timeline.setItems(items);
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