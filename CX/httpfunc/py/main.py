from flask import escape

response = '''
<html>
  <head>
    <!--Load the AJAX API-->
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">

      // Load the Visualization API and the corechart package.
      google.charts.load('current', {'packages':['corechart']});

      // Set a callback to run when the Google Visualization API is loaded.
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          ['Diameter', 'Age'],
          [8, 37], [4, 19.5], [11, 52], [4, 22], [3, 16.5], [6.5, 32.8], [14, 72]]);

        var options = {
          title: 'Age of sugar maples vs. trunk diameter, in inches',
          hAxis: {title: 'Diameter'},
          vAxis: {title: 'Age'},
          legend: 'none',
          trendlines: { 0: {} }    // Draw a trendline for data series 0.
        };

        var chart = new google.visualization.ScatterChart(document.getElementById('chart_div'));
        chart.draw(data, options);
      }
    </script>
  </head>

  <body>
    <!--Div that will hold the pie chart-->
    <h3>py google plot</h3>
    <div id="chart_div"></div>
  </body>
</html>
'''


def py_http_gplot(request):
    return response


def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'World'
    return 'Hello {}!'.format(escape(name))

# https://googleapis.dev/python/datastore/latest/client.html
# Imports the Google Cloud client library
from google.cloud import datastore
def http_datastore(request):
    # Instantiates a client
    datastore_client = datastore.Client()

    query = datastore_client.query(kind='Task')
    # query.order = ['-timestamp']

    data = query.fetch(limit=10)

    return str([x['value'] for x in data])

    # The kind for the new entity
    # kind = "Task"
    # The name/ID for the new entity
    # name = "sampletask1"
    # The Cloud Datastore key for the new entity
    # task_key = datastore_client.key(kind, name)

    # Prepares the new entity
    # task = datastore.Entity(key=task_key)
    # task["description"] = "Buy milk"

    # Saves the entity
    # datastore_client.put(task)

    # print(f"Saved {task.key.name}: {task['description']}")