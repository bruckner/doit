var form = $('#field-search-form');
var baseURL = $(form).attr('action');
var exact = $(form).find('input[type="checkbox"]').is(':checked');
var exactPath = exact ? 'named/' : 'like/'
var query = $(form).find('input[type="text"]').val();

$(form).submit(function (e) {
    e.preventDefault();
    var exact = $(form).find('input[type="checkbox"]').is(':checked');
    var exactPath = exact ? 'named/' : 'like/'
    var query = $(form).find('input[type="text"]').val();
    location.href = baseURL + exactPath + query + '/map';
});
