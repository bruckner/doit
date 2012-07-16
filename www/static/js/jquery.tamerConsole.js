/* Where are we? */
var baseURL = '/tamer/goby/';


/* Nav bar funcionality */
var $nav = $('.tamer-console-nav'),
    $new = $nav.find('.new');

$nav.accordion({
    autoHeight: false,
    collapsible: true
});

$new.find('a')
    .first().click(function (e) {
    }).end()
    .last().click(function (e) {
        
    });

var $navLinks = $nav.find('a');

$nav.find('a[name="redirect"]').each(function () {
    var $this = $(this);
    $this.click(function (e) {
        e.preventDefault();
        var url = $this.attr('href'),
            data = {},
            callback = function (json) {
                location.href = json.redirect;
            }
            $.post(url, data, callback);
    });
});


/* Table import form */
var $importForm = $('.tamer-import-form'),
    $tableSelect = $importForm.find('.table-selector'),
    $attributeSelect = $importForm.find('.attr-selector'),
    $importSubmit = $importForm.find('input[type="submit"]');

$importForm.submit(function (e) {
    e.preventDefault();
    var $this = $(this),
        tableName = $tableSelect.find(':selected').text(),
        $importStatus = $this.find('.import-status'),
        url = $this.attr('action'),
        data = {
            'schemaname': 'public',
            'tablename': tableName
        },
        callback = function (json) {
            $importStatus.text(json.status);
            location.reload(true);
        },
        eidattr, sidattr, error,
        dataattr = new Array();

    $attributeSelect.find('select').each(function () {
        var $this = $(this),
            attrName = $this.closest('tr').children().first().text();
        if ($this.val() === 'import as data') {
            dataattr.push(attrName);
            return;
        }
        if ($this.val() === 'use as entity key') {
            if (eidattr !== undefined) {
                error = 'Multiple entity keys are not allowed.'
                return false;
            } else
                eidattr = attrName;
            return;
        }
        if ($this.val() === 'use as source key') {
            if (sidattr !== undefined) {
                error = 'Multiple source keys are not allowed.'
                return false;
            } else
                sidattr = attrName;
            return;
        }
    });

    if (error !== undefined) {
        alert(error);
        return;
    }

    data['dataattr'] = dataattr.join(',');
    if (eidattr)
        data['eidattr'] = eidattr;
    if (sidattr)
        data['sidattr'] = sidattr;

    $importStatus.html('Loading...');
    $.post(url, data, callback);
});

$tableSelect.change(function (e) {
    var tableName = $tableSelect.find(':selected').text(),
        url = baseURL + 'widgets/attribute-selector',
        data = {'tablename': tableName},
        callback = function () {
            $importSubmit.val('Import table ' + tableName).removeAttr('disabled');
        };

    if (tableName)
        $attributeSelect.load(url, data, callback)
    else {
        $attributeSelect.html('');
        $importSubmit.val('Import table...').attr('disabled', 'disabled');
    }
});


// Auxiliary table import forms
var $auxForms = $('.tamer-import-aux-form'),
    $auxTableSelect = $auxForms.find('.table-selector');

$auxTableSelect.change(function (e) {
    var $this = $(this),
        tableName = $this.find(':selected').text(),
        $form = $this.closest('form'),
        $submit = $form.find('input[type="submit"]'),
        $radio = $form.find('.attr-radio');

        $submit.removeAttr('disabled');

        $radio.each(function () {
            var $this = $(this),
                url = baseURL + 'widgets/attribute-radio',
                data = {'tablename': tableName, 'name': $this.attr('name')},
                callback = function () {};

             if (tableName)
                $this.load(url, data, callback);
             else
                $submit.attr('disabled', 'disabled');
        });
});

$auxForms.submit(function (e) {
    e.preventDefault();
    var $this = $(this),
        $status = $this.find('.import-status'),
        url = $this.attr('action'),
        data = $this.serialize(),
        callback = function (json) {
            $status.html(json.status);
        };
        $.post(url, data, callback);
});


/* Source action page */
var $sourceActionForm = $('.tamer-source-action-form');

$sourceActionForm.submit(function (e) {
    e.preventDefault();
    var $this = $(this),
        url = $this.attr('action'),
        data = {},
        callback = function (json) {
            location.href = json.redirect;
        }
    $.post(url, data, callback);
});


/* De-dup init page */
var $initDedupForm = $('.init-dedup-form');

function initDedupAddSelect (e) {
    var $this = $(this),
        currIndex = $this.attr('name').split('-')[1],
        newIndex = parseInt(currIndex) + 1,
        $currDiv = $this.parent(),
        $newDiv = $currDiv.clone(true),
        $newSelect = $newDiv.find('select');

    $newSelect.attr('name', $newSelect.attr('name').replace(currIndex, newIndex));
    $this.unbind('change');
    //$newSelect.change(initDedupAddSelect);
    $currDiv.after($newDiv);
}

$initDedupForm.find('select')
    .change(initDedupAddSelect)
    .prev().click(function (e) {
        var $this = $(this),
            $parent = $this.parent(),
            $select = $parent.find('select');
        if ($select.attr('name').split('-')[1] != '0') {
            if ($select.data('events') &&
                $select.data('events').change[0].handler === initDedupAddSelect)
                $parent.prev().find('select').change(initDedupAddSelect);
            $parent.remove();
        }
    });


$initDedupForm.submit(function (e) {
    e.preventDefault();
    var $this = $(this),
        url = $this.attr('action'),
        data = $this.serialize(),
        callback = function (jsonResponse) {
            location.href = 'train-dedup';
        };
    $.post(url, data, callback);
});







