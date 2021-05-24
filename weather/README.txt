My main ASSUMPTIONS are 

1. there is a single external "weather" file per day 
2. there can be duplicates in the data (i.e, within the file the same row can be repeated) - this is handled by deduping
3. there cand be nulls in the incoming data - but we are truly only interested in these attributes "ObservationDate", "ScreenTemperature","Region"
   so, if any of these are NULL, then those rows are dumped in an error file with a filename input\\error\\weather.error.<yyyymmdd>.csv
4. I am storing the output data in a file "C:\\weather\\output\\overall\\weather.overall.csv"
5. also, the same "high" temperature can occur at more than one place on the same day or different dates
   if such a thing happens, then all the rows which are unique w.r.t "ObservationDate"+"ScreenTemperature"+"Region" will be found in the "overall" file



The main program is \weather\weather_tasks\weather_tasks.py

This has 2 classes viz. WeatherDataImportTask and WeatherTask

Being luigi classes, the way to invoke this is 

python -m luigi --module weather.weather_tasks WeatherTask --date 2021-01-02 --local-scheduler 

I have divided the directory structure for the datapipeline as follows

the raw file (i.e the files from the Weather agency) are to be welcomed into /input/raw
The python program will read this file and any errors are dumped into /input/error
processed parquet files with just the 3 columns of interest live under /input/processed

Under the /output
we have 

/daily : This stores the HIGHEST temp for the DAY we processed. 
/done  : This directory stores a "marker" file for LUIGI to prevent rerunning if the day is already done
/overall : This is the real output you are interested in - which has the HIGHEST RECORDED temp , it can have more than one row since same "high" temperature can occur 
at more than one place on the same day or different dates