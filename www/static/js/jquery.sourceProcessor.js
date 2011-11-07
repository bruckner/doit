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

(function () {

var form = $('#process-source-form');
var url = $(form).attr('action');

var successCount = 0;

$(form).submit(function (e) {
	e.preventDefault();
	processSource($('input[type="text"]', form).val())

});

function processSource (sourceId) {
	$('.status', form).html('Starting source ' + sourceId)
	for (var i=1; i<5; i++)
		$.post(url + sourceId + '/' + i + '/', updateStatus);
}

function updateStatus (json) {
	var status = $('.status', form);
	$(status).html($(status).html() + '<br>' + json.method + ' finished OK');

	++successCount;
	if (successCount === 4) {
		$(status).html($(status).html() + '<br>Loading composite scores...');
		$.post(url + json.source + '/0/', function (json) {location.href = json.redirect;} );
	}
}

})();
