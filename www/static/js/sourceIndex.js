(function ($) {

var $viewControls = $('.viewControls'),
    $viewAllButton = $viewControls.find('.all'),
    $viewIntegratedButton = $viewControls.find('.integrated'),
    $viewProcessedButton = $viewControls.find('.processed'),
    $sourceRows = $('.textFilter-target');

$viewAllButton.click(function () {
    $sourceRows.removeClass('sourceFilter-hidden');
});

$viewIntegratedButton.click(function () { 
    $sourceRows
        .addClass('sourceFilter-hidden')
        .filter('.integrated')
        .removeClass('sourceFilter-hidden');
});

$viewProcessedButton.click(function () {
    $sourceRows
        .removeClass('sourceFilter-hidden')
        .filter('.integrated')
        .addClass('sourceFilter-hidden');
});

})(jQuery);
