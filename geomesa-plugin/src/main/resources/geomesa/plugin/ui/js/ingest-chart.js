function ingestChart() {
    var margin = { top: 5, right: 5, bottom: 20, left: 45 },
        width = 740 - margin.left - margin.right,
        height = 140 - margin.top - margin.bottom;

    var duration = 5000, // how often we poll for changes, in ms
        transitionDelay = 1000, // animation length
        now = new Date(Date.now() - duration);

    var pixelsPerEntry = 10,
        ticks = 5; // how many ticks for the y axis
    // each entry in our array corresponds to n pixels
    var domain = Math.round(width / pixelsPerEntry);

    // the url to our servlet for getting data via ajax
    var url = "/geoserver/geomesa/ingest/rate?tables=" + tables.map(function(t) { return t.table }).join();

    // start out with an empty array of data so that the chart animates smoothly from the right
    var emptyValues = new Array(domain);
    for (var i = 0; i < domain; i++) {
        emptyValues[i] = 0;
    }

    // array to hold our data - each table gets its own entry
    var ingest = tables.map(function(t) { return { name: t.name, table: t.table, values: emptyValues.slice() }})

    // we use (domain - 2) so that we get smooth interpolation of our curves
    var x = d3.time.scale()
        .range([0, width])
        .domain([now - (domain - 2) * duration, now - duration]);
    var y = d3.scale.linear()
        .range([height, 0])
        .domain([0, 11]);

    var graph = d3.select(".ingest")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");;

    // the clip ensures that the lines animate smoothly by cutting off the most recent part
    graph.append("defs")
        .append("clipPath")
        .attr("id", "clip")
        .append("rect")
        .attr("width", width)
        .attr("height", height);

    var xAxis = graph.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + y(0) + ")")
        .call(x.axis = d3.svg.axis().scale(x).orient("bottom"));

    var yAxis = graph.append("g")
        .attr("class", "y axis")
        .call(y.axis = d3.svg.axis().scale(y).orient("left").ticks(ticks));

    var line = d3.svg.line()
        .x(function(d,i) { return x(now - (domain - 1 - i) * duration); })
        .y(function(d) { return y(d); })
        .interpolate("basis")

    var table = graph.selectAll(".table")
        .data(ingest)
        .enter()
        .append("g")
        .attr("clip-path", "url(#clip)")
        .attr("class", "table");

    table.append("path")
        .attr("class", "line")
        .attr("d", function(d) { return line(d.values); })
        .style("stroke", function(d) { return color(d.name); });

    var lines = graph.selectAll(".table .line");

    var max = 10;

    function redraw(data) {
        // add the newest element from the ajax call
        for (var i = 0; i < ingest.length; i++) {
            ingest[i].values.push($.grep(data, function(d) { return d.table == ingest[i].table })[0].ingest);
        }

        // update the domains - note we're also trimming the last 2 values so line interpolation looks ok
        now = new Date();
        x.domain([now - (domain - 2) * duration, now - duration]);

        // only update the y domain if the scale changed (ensure a minimum range of 10)
        var newMax = d3.max([10, d3.max(ingest, function(d) { return d3.max(d.values); })]);
        if (newMax != max) {
            max = newMax;
            y.domain([0, max + Math.floor(max / 10)]);
            yAxis.transition()
                .duration(transitionDelay)
                .ease("linear")
                .call(y.axis);
        }

        // transition the x axis to display the current time
        xAxis.transition()
            .duration(transitionDelay)
            .ease("linear")
            .call(x.axis);

        // transition the line by applying new values and then translating them into view
        lines.attr("transform", null) // clear any existing transform
            .attr("d", function(d) { return line(d.values); }) // apply the new data values
            .transition() // start a transition to bring the new value into view
            .duration(transitionDelay)
            .ease("linear")
            .attr("transform", "translate(" + x(now - (domain - 1) * duration) + ")");

        // remove the oldest element that slid off the left
        for (var i = 0; i < ingest.length; i++) {
            ingest[i].values.shift();
        }

        // set up the next poll call if everything went ok
        setTimeout(ajaxCall, duration);
    }

    function showError(request, status, error) {
        var g = graph.append("g")
            .attr("transform", "translate(" + (width / 2) + "," + (height / 2) + ")");

        // append a background rect so that the text is visible
        var background = g.append("rect")
            .style("fill", "white")
            .style("stroke", "black")
            .style("stroke-width", "1px");

        g.append("text")
            .attr("transform", "translate(0,-10)")
            .style("text-anchor", "middle")
            .style("font-size", "16px")
            .style("font-weight", "bold")
            .text("Ingest rates are not available");
        g.append("text")
            .attr("transform", "translate(0,10)")
            .style("text-anchor", "middle")
            .style("font-size", "12px")
            .text("Please ensure the accumulo monitor is up and running");

        // expand the background to match the text bbox
        var bbox = g.node().getBBox();
        background.attr("x", bbox.x - 10)
            .attr("y", bbox.y - 10)
            .attr("height", bbox.height + 20)
            .attr("width", bbox.width + 20)
    }

    function ajaxCall() {
        $.ajax({ url: url, cache: false, success: redraw, error: showError, dataType: "json" });
    }

    // kick off the first poll for data
    ajaxCall();
}
ingestChart();
