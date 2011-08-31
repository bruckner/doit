
/* match lists  */
var matchers = $('.mapper td.match');
$(matchers).each( function () {
    var openButton = $(this).children().first();
    var list = $('ul', this);
    $(openButton).click( function (e) {
	toggle_match_list(list);
	e.stopPropagation();
    });
});


function open_match_list (mlist) {
    var pos = $(mlist).offset();
    $(mlist)
	.css('top', pos.top)
	.css('left', pos.left)
	.css('position', 'absolute')
	.css('height', 'auto')
	.css('border', '1px solid #3344FF')
	.children()
	.css('padding', '8px 4px 0 4px')
	.hover(
	    function () {
		$(this).css('background-color', '#8899FF').css('color', 'white');
	    },
	    function () {
		$(this).css('background-color', 'white').css('color', 'black');
	    })
	.click( function (e) {
	    $(this).detach().prependTo(mlist);
	    close_match_list(mlist);
	    e.stopPropagation();
	})
	.show();
    $('body')
	.click( function () {
	    close_match_list(mlist);
	});
}

function close_match_list (mlist) {
    $(mlist)
	.click( function (mlist) {
	    //open_match_list(mlist);
	})
	.children()
	.hide()
	.first()
	.unbind('mouseenter')
	.unbind('mouseleave')
	.unbind('click')
	.css('background-color', 'white')
	.css('color', 'black')
	.css('padding', '10px 4px 4px 4px')
	.show()
	.end()
	.end()
	.css('position', 'static')
	.css('height', 'auto')
	.css('border', 'none');
}

function toggle_match_list (mlist) {
    if ($(mlist).css('position') === 'absolute')
	close_match_list(mlist);
    else
	open_match_list(mlist);
}



/* action buttons (coming soon...) */
var actions = $('.actions');
var accept_buttons = $('.accept', actions);
var reject_buttons = $('.reject', actions);
var reset_buttons  = $('.reset',  actions);

$(accept_buttons).each( function () {
    var container = $(this).parent().closest('div');
    $(this).click( function () {
	$(container)
	    .css('background', '-webkit-gradient(linear, 100% 0, 0 0, from(#AFA), to(white))')
	    .find('li')
	    .first()
	    .addClass('mapped')
	    .end()
	    .end()
	    .find('.status')
	    .text('mapped')
	    .end()
	    .find('.button')
	    .hide()
	    .filter('.reset, .detail')
	    .show();
    });
});

$(reject_buttons).each( function () {
    var container = $(this).parent().closest('div');
    $(this).click( function () {
	var mlist = $(container).find('ul');
	$(mlist).children()
	    .first()
	    .hide()
	    .next()
	    .show()
	    .end()
	    .detach()
	    .appendTo(mlist);
    });
});

$(reset_buttons).each( function () {
    var container = $(this).parent().closest('div');
    $(this).click( function () {
	$(container)
	    .css('background', '-webkit-gradient(linear, 100% 0, 0 0, from(#FAA), to(white))')
	    .find('li')
	    .first()
	    .removeClass('mapped')
	    .end()
	    .end()
	    .find('.status')
	    .text('unmapped')
	    .end()
	    .find('.button')
	    .show()
	    .filter('.reset')
	    .hide();
    });
});


/* floating control panel */
var panel = $('.floatcontrol');
$(panel).submit(function (e) { e.preventDefault(); });

$('[value="Reset"]', panel).click( function () {
    $(reset_buttons).click();
});

$('[value="Save"]', panel).click( function () {
    var mappings = {};
    $('.mapped', matchers).each( function () {
	var mapping = $(this).attr('id').split('-to-');
	mappings[mapping[0]] = mapping[1];
    });
    $.post('./save', mappings, function (d) {
	alert('Saved OK\n' + d);
    });
});




/* detailed views */
var detail_buttons = $('.detail', actions);

$(detail_buttons).each( function () {
    var attr_name = encodeURIComponent($(this).closest('tr').children().first().text());
    $(this).click( function () {
	fill_popover('./!' + attr_name + '/summary', 600);
    });
});

function open_popover (width) {
    var body = $('body')

    width = width || 600;

    return $('body')
	.append('<div class="dimmer"></div><div class="popover"></div>')
	.find('.dimmer')
	.click( function () {
	    close_popover();
	})
	.end()
	.find('.popover')
	.css('top', ($(window).scrollTop() + 40) + 'px')
	.css('left', ($(window).scrollLeft() + ($(window).width() - width) / 2) + 'px')
	.css('width', width);
}

function close_popover () {
    $('.dimmer').remove();
    $('.popover').remove();
}

function fill_popover (url, width) {
    close_popover();
    var pop = $(open_popover(width));

    $(pop).html('<p>Loading...</p>');
    $.get(url, function (d) {
	$(pop).html(d);
    });

    return $(pop);
}

