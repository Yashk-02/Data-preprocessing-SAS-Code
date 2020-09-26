## Code to Fetch Station details using Bike Station location CSV

proc sql;
%if %sysfunc(exist(WORK.IMPORT)) %then %do;
    drop table WORK.IMPORT;
%end;
%if %sysfunc(exist(WORK.IMPORT,VIEW)) %then %do;
    drop view WORK.IMPORT;
%end;
quit;



FILENAME REFFILE FILESRVC FOLDERPATH='/UCD Project'  FILENAME='BIKE_STATIONS_LOCATION.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=WORK.IMPORT;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=WORK.Stations; RUN

## Promoting code to SAS CAS Server

options cashost="localhost" casport=5570;
cas;
libname pubCAS cas caslib=public;
proc casutil;
			load data=WORK.stations outcaslib="public"
			casout="Dublin_bikes_stations" promote;
run;




## Code to fetch 115 Dublin bikes stations data from public API##

proc print data = pubcas.dublin_bikes_stations;
run; 

%macro test(x);
%put &x;
filename response temp encoding="utf-8" lrecl=1000000;
proc http
url="https://dublinbikes.staging.derilinx.com/api/v1/resources/historical/?dfrom=201908011200&dto=202006011200&station=&x."
method= "GET"
out=response;
run;
libname DSURVEYS JSON fileref=response;
options cashost="localhost" casport=5570;
cas;
libname pubCAS cas caslib=public;
%if not %sysfunc(exist(pubCAS.JK_test_data)) %then %do;
		proc casutil;
			load data=DSURVEYS.HISTORIC outcaslib="public"
			casout="JK_test_data" promote;
		quit;
	%end;

	%else %do;
		proc casutil;
			load data=DSURVEYS.HISTORIC outcaslib="public"
			casout="JK_test_data" append;
		quit;
%end;
%mend test;

proc sql;
select cats('%test(',x,')') into :testcall separated by ' ' from pubcas.dublin_bikes_stations;
quit;

&testcall;


