
/* 
exports.http_function = (request, response) => {

  // Imports the Cloud Datastore client library
  const Datastore = require('@google-cloud/datastore');

  // Your Google Cloud Platform project ID
  const projectId = process.env.GCLOUD_PROJECT;

  // Creates a client
  const datastore = new Datastore({
    projectId: projectId,
  });

  // Define the query: select the most recent record
  const query = datastore
    .createQuery('TrendingHashtags')
    .order('datetime', {
      descending: true,
    })
    .limit(1);

  // Execute the query
	datastore.runQuery(query).then(results => {
    var responseString = '<html><head><meta http-equiv="refresh" content="5"></head><body>';
    const entities = results[0];

    if (entities.length && entities[0].hashtags.length) {
      responseString += '<h1>Trending hashtags</h1>';
      responseString += '<ol>';
      entities[0].hashtags.forEach(hashtag => {
        responseString += `<li>${hashtag.name} (${hashtag.occurrences})`;
      });
      response.send(responseString);
    }
    else {
      responseString += 'No trending hashtags at this time... Try again later.';
      response.send(responseString);
    }
  })
  .catch(err => {
    response.send(`Error: ${err}`);
  });
};
*/

exports.http_function = (request, response) => {

  const responseString = `<html>
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
    >
    <div id="chart_div"></div>
  </body>
</html>`;

  // Execute the query
  response.send(responseString);
};