// Load the Visualization API and the piechart package.
google.load('visualization', '1', {'packages':['corechart']});
      
var classArray = {};
var header = [];
updateTransferTable = function() {
    var jFile = 'class_history.json';
    var table = $('#table');
    table.empty();
    table.css('text-align','left');
    var tbody = $('<tbody>');
    tbody.appendTo(table);

    $.getJSON(jFile, function(data) {
	header = data['release'];
	classArray = data['classes'];
	counts = data['counts'];
	var tr = $('<tr>');
	
	$('<th>').html('Release').appendTo(tr);
	$.each(header, function(i,h) {
	    $('<th>').html(h).appendTo(tr);
	});
	tr.appendTo(tbody);

	$.each(classArray, function(i,rclass) {
	    tr = $('<tr>');
	    th = $('<th>');
	    l  = $('<label />').html(' '+rclass);
	    cb = $('<input/>').attr({ type: 'checkbox', id: rclass});
	    if (rclass == 'Pathway' || rclass == 'Reaction') {
		cb.prop('checked', true);
	    }
	    l.prepend(cb);
	    
	    cb.click(function(){
		plotClass();
	    });
	    l.appendTo(th);
	    th.appendTo(tr);
	    $.each(counts[rclass], function(i,cnt) {
		$('<td>').html(cnt).appendTo(tr);
	    });
	    tr.appendTo(tbody);
	    
	});
	plotClass();
    });
}

plotClass = function() {

    var chart_data = new google.visualization.DataTable();
    var rows = {};
    chart_data.addColumn('string', 'Release');
    
    $.each(classArray, function(i,rclass) {
        if ($('#'+rclass).is(":checked")) {
	
	    $.each(header, function(i,r) {
		if (!rows[r]) {
		    rows[r] = [];
		}
		num = parseInt(counts[rclass][i]);
		rows[r].push(num);
	    });
	    
	    chart_data.addColumn('number', rclass);
	}
    });

    $.each(rows, function(r,row) {
	row.unshift(r);
	chart_data.addRow(row);
    });

    
    var options = {
        title: 'Number of class instances by release',
        vAxis: {title: 'Count', textPosition: 'in'},
	hAxis: {title: 'Release'},
        legend: { position: 'right'},
        width: '100%',
        height: 600,
        pointSize: 5,
        theme: 'material',
        //chartArea:{left:50,top:50,width:'60%',height:'80%'}
    };
    
    $('#plot').empty();
    var chart = new google.visualization.LineChart(document.getElementById('plot'));
    chart.draw(chart_data, options);
}

