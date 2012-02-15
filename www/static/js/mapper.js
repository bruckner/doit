var rPath = /^\/doit\/\w+\//;
var basePath = rPath.exec(location.pathname).toString();

/* layout */
var topPane = $('.pane.top'),
    leftPane = $('.pane.left'),
    rightPane = $('.pane.right');

function setPaneHeights () {
    var rightExtraHeight = $(rightPane).outerHeight(true) - $(rightPane).height(),
        leftExtraHeight = $(leftPane).outerHeight(true) - $(leftPane).height();
    $(rightPane).height($(window).height() - $(topPane).outerHeight(true) - rightExtraHeight);
    $(leftPane).height($(rightPane).height() - leftExtraHeight);
}

function setMapperDimensions () {
    var $mapper = $('.mapper'),
        mapperMargin = $mapper.outerWidth(true) - $mapper.outerWidth(false)
        scrollBarWidth = 16; // Just an approx.
    $mapper.width(rightPane.innerWidth() - mapperMargin - scrollBarWidth);
}

$(window).resize(setPaneHeights);
$(window).resize(setMapperDimensions);
$(window).resize();

/* match menu lists  */
var matchers = $('.mapper td.match');

$(matchers).each(function () {
    var openButton = $('.button', this);
    var list = $('.map-list', this);
    $(openButton).click(function (e) {
        if ($(openButton).is('.disabled'))
            return;
 	e.stopPropagation();
	toggle_match_list(list);
    });
});

function get_match_list(mlist, field_id, afterLoad) {
    afterLoad = (typeof afterLoad === 'function') ? afterLoad : function () {};
    var url = basePath + 'fields/' + field_id + '/candidates/',
        data = {},
        callback = function (responseText) {
            $(mlist).html(responseText);
            init_match_list(mlist);
            afterLoad();
        };
    $(mlist).html('<p>Loading...</p>');
    $.get(url, data, callback);
}

function init_match_list(mlist) {
    var container = $(mlist).closest('.map-list-container');
    var pos = $(container).prev().offset();
    if (pos.top - 32 + $(container).outerHeight() > $(window).height())
        $(container).css('top', $(window).height() - 280);

    $('.candidate', mlist).click(function () {
        $('.selected').removeClass('selected');
        $(this).addClass('selected');
        update_mapping_choice(mlist);
        close_match_list(mlist);
    });
}

function open_match_list (mlist) {
    if ($('.map-list-container.open').length)
        return;
    var container = $(mlist).closest('.map-list-container');
    var pos = $(container).prev().offset();
    $(container)
        .addClass('open')
        .css('top', pos.top - 32)
        .css('left', pos.left + 16);

    $(mlist).closest('tr').find('.button').addClass('disabled');
    $(container).click(function (e) {e.stopPropagation();});
    $('body')
	.click( function () {
	    close_match_list(mlist);
	});
}

function close_match_list (mlist) {
    $(mlist).closest('.map-list-container')
        .removeClass('open')
        .css('top', 'auto')
        .css('left', 'auto')
        .closest('tr')
            .find('.button')
                .removeClass('disabled');
}

function toggle_match_list (mlist) {
    if (!$(mlist).children().length) {
        var field_id = $(mlist).closest('td').find('.choice')
                           .attr('id').split('-is-')[0];
        get_match_list(mlist, field_id);
    }
    if ($(mlist).closest('.map-list-container').is('.open'))
	close_match_list(mlist);
    else
	open_match_list(mlist);
}

function update_mapping_choice (mlist) {
    var choice = $('.selected', mlist);
    var mapping = $(choice).attr('id').split('-to-');
    var fromId = mapping[0];
    var toId = mapping[1];
    var name = $(choice).text();
    var borderColor = $(choice).css('border-left-color');
    var title = $(choice).attr('title');
    var target = $(choice).closest('td').find('.choice');
    $(target)
        .text(name)
        .css('border-left-color', borderColor)
        .attr('id', fromId + '-is-' + toId)
        .attr('title', title);
}

/* match list filters */
$('.map-list-container')
    .find('.filter')
    .children()
    .focus(function () {
        if ($(this).val() === 'Filter')
            $(this).val('');
    })
    .blur(function () {
        if ($(this).val() === '')
            $(this).val('Filter');
    })
    .keyup(function () {
        update_filter(this);
    });

function update_filter (inputEl) {
    var patterns = $(inputEl).val().toLowerCase().split(' ');
    var listElems = $(inputEl).closest('.map-list-container').find('.candidate');
    if (!patterns.length)
        $(listElems).show();
    else
        $(listElems)
            .hide()
            .filter(function () {
                for (var i=0; i<patterns.length; i++)
                    if ($(this).text().toLowerCase().indexOf(patterns[i]) === -1)
                        return false;
                return true;
            })
            .show();
}

/* match list "suggest new..." buttons */
var $suggButtons = $('.map-list-container .suggest');

$suggButtons.click(function () {
   var $this = $(this),
       $row = $this.closest('.mapper-row'),
       fid = $row.attr('id').substr(3),
       fname = $row.find('.attr').text(),
       url = basePath + 'suggest-new-attribute/form?fid=' + fid + '&fname=' + fname,
       width = 600,
       callback = function () {
           var $suggestForm = $('.popover form');
           $suggestForm.submit(function (e) {
               e.preventDefault();
               var url = basePath + 'suggest-new-attribute/',
                   data = $suggestForm.serialize(),
                   callback = function () {
                       alert('Ok!');
                       close_popover();
                   };
               $.post(url, data, callback);
           });
       };
   fill_popover(url, width, callback); 
});



/* action buttons */
var actions = $('.actions');
var accept_buttons = $('.accept', actions);
var reject_buttons = $('.reject', actions);
var reset_buttons  = $('.reset',  actions);

$(accept_buttons).each( function () {
    var container = $(this).parent().closest('tr');
    $(this).click( function () {
	$(container)			// This attribute's row (tr)
	    .addClass('mapped')
            .removeClass('unmapped')
            .find('.match')		// The td with the match list
                .find('.button')	// The match list open button
                    .addClass('disabled')
                .end()
	        .find('.choice')	// The chosen match list item
	           .addClass('new-mapping')
	        .end()
	    .end()
	    .find('.status')	// The attributes status cell
	        .text('mapped')
	    .end();
    });
});

$(reject_buttons).each(function () {
    var $this = $(this),
        $container = $this.parent().closest('tr'),
	$mlist = $container.find('.map-list'),
        currentMapping = $container.find('.choice').attr('id'),
        fieldId = currentMapping.substr(0, currentMapping.indexOf('-'));
    $this.click( function () {
        function selectNextChoice () {
	    $('.selected', $mlist)
                .removeClass('selected')
                .addClass('rejected')
                .next()
                    .addClass('selected');
            if (!$('.selected', $mlist).length)
                $('.candidate', $mlist).first().addClass('selected');
            update_mapping_choice($mlist);
        }
        if (!$mlist.find('.candidate').length)
            get_match_list($mlist, fieldId, selectNextChoice);
        else
            selectNextChoice();
    });
});

$(reset_buttons).each( function () {
    var $container = $(this).parent().closest('tr');
    if ($container.find('.choice.mapping').length)
        return;
    $(this).click( function () {
	$container
            .removeClass('mapped')
            .addClass('unmapped')
            .find('.match')
                .find('.button')
                    .removeClass('disabled')
                .end()
	        .find('.choice')
	            .removeClass('new-mapping')
                    .removeClass('mapping')
	        .end()
                //.find('.rejected')
                //    .removeClass('rejected')
                //.end()
	    .end()
	    .find('.status')
	        .text('unmapped')
	    .end();
    });
});


/* control panel */
var $saveButton = $('.control.save', topPane),
    $resetButton = $('.control.reset', topPane),
    $mapallButton = $('.control.mapall', topPane),
    $viewTableButton = $('.control.viewTable-button', topPane);

$resetButton.click(function () {
    $(reset_buttons).click();
});

$mapallButton.click(function () {
    $(accept_buttons).click();
});

$saveButton.click(function () {
    var mappings = [],
        rejected = [];
    $('.new-mapping', matchers).each(function () {
	mappings.push($(this).attr('id').split('-is-'));
    });
    $('.rejected', matchers).each(function () {
	rejected.push($(this).attr('id').split('-to-'));
    });
    if (mappings.length + rejected.length) {
        var url = basePath + 'save',
            data = 'mappings=' + JSON.stringify(mappings) +
                   '&rejects=' + JSON.stringify(rejected),
            callback = function (d) {
                $('.new-mapping')
                    .removeClass('new-mapping')
                    .addClass('mapping')
                    .removeAttr('style');
                $('.rejected').removeClass('rejected');
	        alert('Saved OK');
            };
        $.post(url, data, callback);
    }
});

$viewTableButton.click(function () {
    var $window = $('.pane.right'),
        $mapper = $('.mapper', $window),
        $table = $('.viewTable', $window);

    if (!$table.length) {
        $table = viewTable.getTable();
        $table.appendTo($window);
    }

    if ($mapper.is(':hidden')) {
        $table.hide();
        $mapper.show();
    } else {
        $mapper.hide();
        $table.show();
    }
});


/* detailed views */
var detail_buttons = $('.detail', actions);

$(detail_buttons).each( function () {
//    var attr_name = encodeURIComponent($(this).closest('tr').children().first().text());
    var fid = $(this).closest('tr').attr('id').slice(3);
    $(this).click( function () {
	fill_popover(basePath + 'fields/' + fid + '/summary', 600);
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

function fill_popover (url, width, callback) {
    close_popover();
    width = width || 600;
    callback = typeof callback === 'function' ? callback : function () {};
    var pop = $(open_popover(width));

    $(pop).html('<p>Loading...</p>');
    $.get(url, function (d) {
	$(pop).html(d);
        callback();
    });

    return $(pop);
}

