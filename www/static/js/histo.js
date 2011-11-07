/* Copyright (c) 2011 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

function add_histo (el, data, min_val, max_val) {
    var svg = d3.select(el).append('svg:svg').attr('height', '100%').attr('width', '100%');

    var h = $(el).innerHeight();
    var w = $(el).innerWidth();

    var x = d3.scale.linear().domain([0, data.length]).range([0.1*w, 0.9*w]);
    var y = d3.scale.linear().domain([d3.min(data), d3.max(data)]).range([0.9*h, 0.1*h]);

    var line = d3.svg.line()
	.x(function(d,i) {
            return x(i);
	})
	.y(function(d) {
            return y(d);
	});

    svg.append('svg:path').attr('d', line(data)).classed('histo-line', true);

    var xaxis = d3.svg.line()
	.x( function (d) {
	    return d*w*0.9 + 0.05*w; 
	})
	.y( function () {
	    return 0.92*h
	});

    var yaxis = d3.svg.line()
	.x( function () {
	    return 0.1*h;
	})
	.y( function (d) {
	    return d*h
	});

    //svg.append('svg:path').attr('d', xaxis([0,1])).classed('histo-axis', true);
    //svg.append('svg:path').attr('d', yaxis([0,1])).classed('histo-axis', true);

    // labels
    svg.append('svg:text')
	.attr('x', x(0))
	.attr('y', h*0.95)
	.attr('dx', 0)
	.attr('dy', '0.35em')
	.attr('text-anchor', 'begin')
	.classed('histo-label', true)
	.text(min_val);
    svg.append('svg:text')
	.attr('x', x(data.length))
	.attr('y', h*0.95)
	.attr('dx', -3)
	.attr('dy', '0.35em')
	.attr('text-anchor', 'end')
	.classed('histo-label', true)
	.text(max_val);
}

