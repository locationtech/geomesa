function entriesChart() {
    var margin = { top: 5, right: 10, bottom: 20, left: 80 },
        width = 740 - margin.left - margin.right,
        barHeight = 20,
        height = barHeight * entries.length;

    var x = d3.scale.linear()
        .range([1, width])
        .domain([1, d3.max(entries, function(d) { return d.value + 1; })]);
    var y = d3.scale.ordinal()
        .rangeRoundBands([height, 0], .1)
        .domain(entries.map(function(d) { return d.name; }));

    var chart = d3.select(".entries")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    chart.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.svg.axis().scale(x).orient("bottom"));
    chart.append("g")
        .attr("class", "y axis")
        .call(d3.svg.axis().scale(y).orient("left"));

    chart.selectAll(".bar")
        .data(entries)
        .enter()
        .append("rect")
        .attr("class", "bar")
        .attr("x", 1)
        .attr("y", function(d) { return y(d.name); })
        .attr("height", barHeight - 2)
        .attr("width", function(d) { return x(d.value + 1); })
        .style("fill", function(d) { return color(d.name); });
}
entriesChart();