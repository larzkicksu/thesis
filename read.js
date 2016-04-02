var csv = require('csv-parser');
var fs = require('fs');
var _ = require('lodash');
var output ='';
var codes = [
  2741, 51223,
  3652, 334612, 51222,
  3663, 33422,
  3695, 334613,
  3930,
  3931, 339992,
  5730,
  5731, 443112,
  5735, 45122,
  5736, 45114,
  6512, 71131,
  7310,
  7311, 54181,
  7312, 54185,
  7313, 54184,
  7319, 54183, 54187, 54189,
  7383, 51411,
  7389, 51224, 51229, 71141,
  7920,
  7819,
  7920,
  7922, 71132, 71151,
  7929, 71113, 71119,
  7941,
  7990,
  7999,
  8999
];


var countyFiles = [
  '1988_ALL_Washington_State_Counties.csv',
  '1989_All_Washington_State_Counties.csv',
  '1990_All_Washington_State_Counties.csv',
  '1991_All_Washington_State_Counties.csv',
  '1992_All_Washington_State_Counties.csv',
  '1993_All_Washington_State_Counties.csv',
  '1994_All_Washington_State_Counties.csv',
  '1995_All_Washington_State_Counties.csv',
  '1996_All_Washington_State_Counties.csv',
  '1997_All_Washington_State_Counties.csv',
  '1998_All_Washington_State_Counties.csv',
  '1999_All_Washington_State_Counties.csv',
  '2000_All_Washington_State_Counties.csv'
];

var totals = {};
var endCount= 0;

_.each(countyFiles, function(file) {
  endCount++;
  totals[file] = {
    detailEmp: 0,
    detailEst: 0,
    rows: 0,
    filteredRows: 0,
    emp: 0,
    est: 0,
    counties: {}
  };

  fs.createReadStream(file)
    .pipe(csv())
    .on('data', function(data) {
      totals[file].rows++;
      if (!totals[file].counties[data.fipscty]) {
        totals[file].counties[data.fipscty] = {
          detailEmp: 0,
          detailEst: 0,
          emp: 0,
          est: 0
        };
      }

      if (data.sic) {
        if (codes.indexOf(parseInt(data.sic)) > -1) {
          totals[file].filteredRows++;
          totals[file].detailEmp += parseInt(data.emp);
          totals[file].detailEst += parseInt(data.est);
          totals[file].counties[data.fipscty].detailEmp += parseInt(data.emp);
          totals[file].counties[data.fipscty].detailEst += parseInt(data.est);
        }
      }
      else if (data.naics) {
        if (codes.indexOf(parseInt(data.naics)) > -1) {
          totals[file].filteredRows++;
          totals[file].detailEmp += parseInt(data.emp);
          totals[file].detailEst += parseInt(data.est);
          totals[file].counties[data.fipscty].detailEmp += parseInt(data.emp);
          totals[file].counties[data.fipscty].detailEst += parseInt(data.est);
        }
      }
      totals[file].emp += parseInt(data.emp);
      totals[file].est += parseInt(data.est);
      totals[file].counties[data.fipscty].emp += parseInt(data.emp);
      totals[file].counties[data.fipscty].est += parseInt(data.est);
    })
    .on('end', function() {
      if (--endCount === 0) {
        showLocationQuotiets();
        fs.writeFileSync('Results.csv', output);
      }

    });
});

function showLocationQuotiets() {
  output = 'County,Year,Employees,Establishments,LQ Employees,LQ Establishments' +'\n'
  for (var i = 0; i < countyFiles.length; i++) {
    console.log('\n\n==========================================================');
    console.log(countyFiles[i]);
    console.log('==========================================================');
    console.log(totals[countyFiles[i]]);
    _.each(totals[countyFiles[i]].counties, function(county, countyId) {
      var parts = [];
      var stateTotals = totals[countyFiles[i]];
      var lqEmp = locationQuotient(county.detailEmp, county.emp, stateTotals.detailEmp, stateTotals.emp);
      var lqEst = locationQuotient(county.detailEst, county.est, stateTotals.detailEst, stateTotals.est);
//every time we push to parts, we are creating a column in hte spredsheet
      parts.push(countyId);
      parts.push(parseInt(countyFiles[i]));
      parts.push(county.detailEmp)
      parts.push(county.detailEst)
      parts.push(lqEmp);
      parts.push(lqEst);
      console.log(parts.join('\t'));
      output += parts.join(',') + '\r\n';
    });
  }
}

function locationQuotient(countyDetail, countyAll, stateDetail, stateAll) {
  // console.log(arguments);
  // Location quotient as defined
  //king county detailed industry divided by king county all industry.
  //Divided by Washington State detailed industry divided by Washingtong State all industry
  return (( countyDetail / countyAll) / ( stateDetail / stateAll ));
}
