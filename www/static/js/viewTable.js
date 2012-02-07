var viewTable = (function ($) {

var rPath = /^\/doit\/\w+\//,
    basePath = rPath.exec(location.pathname).toString();

return {
    /* Data members */
    didInit: false,
    $template: null,
    $table: null,
    $grid: null,
    fields: null,
    entities: [],

    /* Methods */
    init: function (sourceId) {

        var url = basePath + 'sources/' + sourceId + '/table',
	    data = {
            },
            table = this,
	    callback = function (responseText) {
                table.$table = $(responseText);
            };
	$.get(url, data, callback);
        return;

	this.getTableTemplate();
        this.getData(sourceId, false, 10);
	this.didInit = true;
    },

    /* Get table template */
    getTableTemplate: function () {
	var url = '/doit/templates/viewTable',
	    data = {},
            table = this,
	    callback = function (responseText) {
		table.$template = $(responseText);
	    };
	$.get(url, data, callback);
    },

    /* Add data to cache */
    getData: function (sourceId, entitiesOnly, nEntities) {
	var url = basePath + 'sources/' + sourceId + '/',
	    data = {
                n: nEntities || 10
            },
            table = this,
	    callback = function (json) {
                table.entities = table.entities.concat(json.entities);
                if (json.fields)
                    table.fields = json.fields;
            };
        url += entitiesOnly ? 'entities' : 'data';
	$.get(url, data, callback);
    },

    /* Build table from data and template */
    getTable: function () {
        return this.$table;
        if (!this.didInit)
            return;
        var $table = this.$template,
            i, $cell;

        /* Column labels */
        $cell = $table.find('.col-label');
        for (i = 0; i < this.fields.length; i++) {
            $cell.text(this.fields[i].name);
            $cell = $cell.clone().appendTo($cell.parent());
        }
        $cell.remove();

        /* Row labels */
        $cell = $table.find('.row-label');
        for (i = 0; i < this.entities.length; i++) {
            $cell.text(this.entities[i].id);
            $cell = $cell.parent().clone()
                        .appendTo($cell.closest('tbody'))
                        .children();
        }
        $cell.remove();
        return $table;
    }
};

})(jQuery);