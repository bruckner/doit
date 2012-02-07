/* textFilter - a simple module for a text input filter.
 * Author -- Daniel Bruckner 2011
 *
 * To use:
 * 1. Include this file on your page, probably at the bottom.
 * 2. Add the class "textFilter-input" to any text input tag you would like to
 *    act as a filter.
 * 3. Add the class "textFilter-target" to any DOM element to be hidden if it
 *    does not match the filter text.
 * 4. Add the class "textFilter-match" to the DOM element (contained by a
 *    textFilter-target) that contains the match text for its ancestor.
 *
 */
(function () {

/* Initialize filter inputs */
var defaultText = $('.textFilter-input').val();

$('.textFilter-input')
	.focus(function (e) {
		if ($(this).val() === defaultText)
			$(this).val('');
	})
	.blur(function (e) {
		if ($(this).val() === '')
			$(this).val(defaultText);
	})
	.keyup(function (e) {
		var patterns = $(this).val().toLowerCase().split(' ');
		if (!patterns.length)
			return;
		$('.textFilter-target')
			.addClass('textFilter-hidden')
			.filter(function () {
				var matchText = $(this)
					.find('.textFilter-match')
					.text()
					.toLowerCase();
				for (var i=0; i<patterns.length; i++)
					if (matchText.indexOf(patterns[i]) === -1)
						return false;
				return true;
			})
			.removeClass('textFilter-hidden');
	});
	
})();

