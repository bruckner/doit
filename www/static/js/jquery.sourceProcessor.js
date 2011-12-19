(function () {

var form = $('#process-source-form');
var baseURL = $(form).attr('action');

var successCount = 0;

$(form).submit(function (e) {
	e.preventDefault();
	processSource($('input[type="text"]', form).val())

});

function processSource (sourceId) {
	$('.status', form).html('Starting source ' + sourceId)
	for (var i=1; i<5; i++)
		$.post(baseURL + sourceId + '/process/' + i + '/', updateStatus);
}

function updateStatus (json) {
	var status = $('.status', form);
	$(status).html($(status).html() + '<br>' + json.method + ' finished OK');

	++successCount;
	if (successCount === 4) {
		$(status).html($(status).html() + '<br>Loading composite scores...');
		$.post(baseURL + json.source + '/process/0/', function (json) {location.href = json.redirect;} );
	}
}

})();
