/*Since the Public API does not contain Station names but only contains station numbers. if
someone wants station name as well with the station number, below can be utilised for such activity*/

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
data Test2;
set Dsurveys.historic(drop=status);
length Station 8;
length Station_name $ 30;
Station = 66;
Station_name = 'NEW CENTRAL BANK';
run;
options cashost="localhost" casport=5570;
cas;
libname pubCAS cas caslib=public;
%if not %sysfunc(exist(pubCAS.Dublin_bikes_data_2020)) %then %do;
		proc casutil;
			load data=Test2 outcaslib="public"
			casout="Dublin_bikes_data_2020" promote;
		quit;
	%end;

	%else %do;
		proc casutil;
			load data=Test2 outcaslib="public"
			casout="Dublin_bikes_data_2020" append;
		quit;
%end;
%mend test;

proc sql;
select cats('%test(',x,')') into :testcall separated by ' ' from pubcas.dublin_bikes_stations;
quit;

&testcall;



/* Changing the time format to SAS time format */

data pubcas.Dublin_bikes_data_2020_CL;
set pubcas.dublin_bikes_data_2020;
length Update $19.;
Update = substr(time,1, 10)||' '||substr(time, 12, 19);
Uptime = input(Update, anydtdtm20.);
format Uptime datetime20.;
run;

/* To split the dataset into Pre-Covid and Covid Dataset */

#Pre-covid dataset
options casdatalimit = 'ALL';
data preCovid;
set pubcas.Dublin_bikes_data_2020;
where Uptime<'01MAR2020:00:00:02'dt;
run;

/* promoting to CAS server */

options cashost="localhost" casport=5570;
cas;
libname pubCAS cas caslib=public;
proc casutil;
	load data=WORK.PRECOVID outcaslib="public" promote
   casout="PRECOVID_BIKE_DATA";


run;


/* Covid Dataset */

options casdatalimit = 'ALL';
data Covid;
set pubcas.Dublin_bikes_data_2020;
where Uptime>'01MAR2020:00:00:02'dt;
run;

/* Promoting to CAS server */

options cashost="localhost" casport=5570;
cas;
libname pubCAS cas caslib=public;
proc casutil;
	load data=WORK.COVID outcaslib="public" promote
   casout="COVID_BIKE_DATA";


run;

/* Removing the night hours in the dataset asa part of Data cleansing as bikes don't operate during night */

data work.peak_covid;
set pubcas.covid_bike_data;
where '05:00:00't<timepart(Uptime)<'23:59:00't;
run;

/* promoting to CAS server */

options cashost="localhost" casport=5570;
cas;
libname pubCAS cas caslib=public;
proc casutil;
	load data=WORK.peak_COVID outcaslib="public" promote
   casout="COVID_BIKE_DATA";


Run;


/* To join weather data with Dublin bikes data */

proc sql;
select w.*, P.*
from weather as w
left join
PRECOVID_BIKE_DATA as P
on w.date = D.uptime
;
run;


proc sql;
select w.*, C.*
from weather as w
left join
COVID_BIKE_DATA as C
on w.date = C.uptime
;
run;

/* To split the data into peak hours and non peak hours */

data pubcas.precovid_date_conversion;
set pubcas.precovid_0808_converted;
Time_con = input(hour, anydtdtm20.);
format Time_con datetime20.;
Run;

options casdatalimit = 'ALL'; 
data work.uncovidcototaltest;
set pubcas.unco_total_date;
length peak_hours 8.;
if '7:00:00't<timepart(Time_con)<'11:00:00't then do;
peak_hours = 1;
end;
else if '16:00:00't<timepart(Time_con)<'20:00:00't then do;
peak_hours = 1;
end;
else peak_hours = 0;
run;

##

/*Connecting to public server in SAS */
options cashost="localhost" casport=5570;
cas;
libname pubCAS cas caslib=public;

/* Increase system data limit to prevent restrictions */
options CASDATALIMIT=ALL;

/* Duplicating and sorting the API bikes data by address and then by the update time */
proc sort data=pubcas.PRECOVID_BIKE_DATA out=work.sortDS equals;
	by Station_name uptime;
run;


/* Removing duplicate rows from the dataset */
proc sort data = WORK.SORTDS out = work.SORTDS1 NODUPKEY;
	by _all_;
run;


/* Re-sorting the newly duplicate free API bike data */
proc sort data = WORK.SORTDS1 out = work.SORTDS2 equals;
	by station_name uptime;
run;

/* Caluculating the change in bikes and stands over time */
data WORK.SORTDS3;
	set WORK.SORTDS2;
	by address;
	/* Dif - gets the difference between the current value and previous value */
	Avail_Bike_Changes = dif(available_bikes);				
	Avail_Bike_Stand_Changes = dif(available_bike_stands);
	if first.address or Time_missed = 1 then do;	/* Gap or 1st appearance of station value reset to 0 */
		Avail_Bike_Changes = 0;
		Avail_Bike_Stand_Changes = 0;
	end;
run;

/* Inserting new column as index of row per station */
data WORK.TDS2;
	set WORK.SORTDS3;
	by address last_update;
	if first.address then x=1;	/* Reset count to 1 for 1st value in each station */
	else x+1;					/* Increase value by 1 for each row */
run;


/* Re-sort data by address and then by most recent update first */
proc sort data = WORK.TDS2;
	by address descending x;
run;


/* Creating new lagged columns */
data WORK.TDS3;
	set WORK.TDS2;
	by address descending x;
	l = lag(x);		/* Copy column and move it down 1 row */
	X_Avail_Bike_Changes = lag(Avail_Bike_Changes);
	X_Avail_Bike_Stand_Changes = lag(Avail_Bike_Stand_Changes);
	if first.station_name then do;	/* Set the 1st values for a new station to 0 */
		l = .;
		X_Avail_Bike_Changes = 0;
		X_Avail_Bike_Stand_Changes = 0;
	end;
run;


/* Re-sort data by address and then by first update */
proc sort data = WORK.TDS3;
	by station_name x;
run;

/* Remove unnecessary columns */
data WORK.SORTDS4;
	set WORK.SORTDS3 (drop =  Avail_Bike_Changes Avail_Bike_Stand_Changes); 
run;

/* Renaming and promoting the data with added activity variables to the public server */
proc casutil;
	load data=WORK.SORTDS4 outcaslib="public" promote
	casout="Precovid_Bike_Activity_Data";
run;

/* This code was used to group the newly created API data file with added 
	activity columns into hours and to calculate the availablity percentage
	for each station at every hour. */


/* Duplicating and sorting the activity data by the update time */
proc sort data=PUBLIC.Precovid_BIKE_ACTIVITY_DATA out=work.hourDS equals;
	by uptime;
run;

/* Duplicating time column - taking only the hour value */
data WORK.HOURDS1;
	set WORK.HOURDS;
	Time_Hour = hms(HOUR('Time'n),0,0);	/* Copying hour values - setting minutes and seconds to 0 */
	format Time_Hour time16.;	/* Formatting new column as SAS time values */
run;


/* Sorting data by station name then by date and then by time */
proc sort data=WORK.HOURDS1 out=WORK.HOURDS2 equals;
	by 'name'n 'Date'n 'Time'n;
run;


/* Creating new columns of nulls for first update in an hour per station */
data WORK.HOURDS3;
	set WORK.HOURDS2;
	by name Date Time_Hour;
	/* First update per hour - change null value to actual value for bikes and stands */
	if First.Time_Hour then do;	
		First_Available_Bikes = available_bikes;
		First_Bike_Stands = available_bike_stands;
	end;
run;


/* Renaming and promoting the hour preparation data to the public server */
proc casutil;
	load data=work.HOURDS3 outcaslib="public" promote
	casout="Precovid_Bike_Hour_Data";
run;


/* Using SQL to group data by hour */
proc sql;
	/* Creating new table to be grouped */
	create table work.testBike as
	/* Selecting only the columns that are needed */
	select 'Date'n, Time_Hour, 'Station_name'n,
	MAX(bike_stands) AS bike_stands, 
	/* Taking the largest or non-null value as available bikes and stands */
	MAX(First_Available_Bikes) AS Available_Bikes, 
	MAX(First_Bike_Stands) AS Available_Bike_Stands,
	/* Sum changes in bikes in an hour for a station */
	SUM(X_Avail_Bike_Changes) AS Avail_Bike_Changes, 
	SUM(Avail_Bikes_Decrease) AS Avail_Bikes_Decrease,
	SUM(Avail_Bikes_Increase) AS Avail_Bikes_Increase,
	/* Sum changes in stands in an hour for a station */
	SUM(X_Avail_Bike_Stand_Changes) AS Avail_Bike_Stand_Changes, 
	SUM(Avail_Stands_Decrease) AS Avail_Stands_Decrease, 
	SUM(Avail_Stands_Increase) AS Avail_Stands_Increase,
	address, banking, Elevation, Latitude, Longitude, 'number'n,
	CASE WHEN 'status'n = "OPEN" THEN "Open" ELSE "Closed" END AS Status
	from PUBLIC.Precovid_Bike_Hour_data /* Calling in the newly created hour preparation dataset */
	/* Using the non-changing variables to group the data */
	group by 'Date'n, Time_Hour, station_name, Elevation,
	 Latitude, Longitude, station;
quit;


/* Adding availabilty and datetime columns */
data WORK.testBike2;
	set WORK.testBike;
	by Date Time_Hour;
	/* New column for availability */
	/* Percentage of availabe bikes per number of stands at a station */
	Availability = Available_Bikes/bike_stands;
	/* New combined column with hour and date */
	Date_Time = dhms(Date, 0, 0, Time_Hour);
	/* Reformatting new column to the SAS datetime format */
	format Date_Time NLDATM30.;
run;

/* Renaming and promoting the hour grouped data to the public server */
proc casutil;
	load data=work.testBike2 outcaslib="public" promote
	casout="Precovid_Bike_Hour_Data";
run;


/* This code was used to group the data by weekday to find the average number of 
	available bikes and changes to be used in the demand calculation. */
/* Duplicating and sorting the hour data with added variables by address and then by update time */
proc sort data=PUBLIC.Precovid_BIKE_HOUR_WEIGHTS2 out=work.WEEKDAY equals;
	by address Date_Time;

/* Creating new column for weekday */
data work.WEEKDAY1;
	set work.WEEKDAY;
	by Station_name Date_Time;
	/* Using weekday function to get weekday number from date column */
	DayofWeek = Weekday('Date'n);
run;


/* Creating new column for weekday names */
data work.WEEKDAY2;
	set work.WEEKDAY1;
	/* Creating column for weekday name based on number in weekday column */
	if DayofWeek = 1 then DayWeek_proc = "Sun";
	if DayofWeek = 2 then DayWeek_proc = "Mon";
	if DayofWeek = 3 then DayWeek_proc = "Tues";
	if DayofWeek = 4 then DayWeek_proc = "Wed";
	if DayofWeek = 5 then DayWeek_proc = "Thur";
	if DayofWeek = 6 then DayWeek_proc = "Fri";
	if DayofWeek = 7 then DayWeek_proc = "Sat";
run;


/* Renaming and promoting the weekday preparation data to the public server */
proc casutil;
	load data=work.WEEKDAY2 outcaslib="public" promote
	casout="Precovid_Weekday_Prep2";
run;

/* Using SQL to group data by hour and day of week */
proc sql;
	/* Creating new table to be grouped */
	create table work.testGroup as
	/* Selecting only the columns that are needed */
	select Time_Hour, station_name,
	/* Calculating and saving average values by hour and weekday */
	AVG(Available_Bikes) AS Avg_Available_Bikes, 
	AVG(Avail_Bike_Changes) AS Avg_Bike_Change, 
	DayofWeek, DayWeek_proc
	/* Calling in the newly created weekday preparation dataset */
	from Public.Precovid_Weekday_Prep2
	/* Grouping the data by address, hour and day of week */
	group by Time_Hour, station_name, DayofWeek, DayWeek_proc;
quit;

/* Re-sort grouped data by address and then by hour */
proc sort data=work.testGroup out=work.testGroup1 equals;
	by station_name Time_Hour;
run;


/* Create new integer rounded average values */
data WORK.testGroup2;
	set WORK.testGroup1;
	by address Time_Hour;
	/* Round average available bikes to an integer in new column */
	Avg_Avail_Bikes = round(Avg_Available_Bikes,1);
	/* Round average available bike changes to an integer in new column */
	Avg_Avail_Bike_Change = round(Avg_Bike_Change,1);
run;

/* Remove unnecessary columns */
data WORK.testGroup3;
	set WORK.testGroup2 (drop = Avg_Available_Bikes Avg_Bike_Change);
run;

/* Renaming and promoting the grouped weekday data to the public server */
proc casutil;
	load data=work.testGroup3 outcaslib="public" promote
	casout="Precovid_Weekday_Data2";
run;









