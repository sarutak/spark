
function drawApplicationTimeline(eventObjArray) {
  var visDataSet = new vis.DataSet(eventObjArray);
  var options = {
    editable: false
  };

  var container = document.getElementById('application-timeline');
  var timeline = new vis.Timeline(container, visDataSet, options);
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