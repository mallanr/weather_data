import os
import re

import luigi
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from workflow.targets import DelegatingTarget


class WeatherDataImportTask(luigi.ExternalTask):
    """
    Weather file received in directory input/raw
    Enrich and put in input/processed [convert to parquet for faster processing]
    Error (any with missing data in Temp/Date/Region) go into input/error
    """
    date = luigi.DateParameter()
    directory = luigi.Parameter(
        default="C:\\weather\\input\\raw")

    def output(self):
        """
        Raises FileNotFoundError if file is not present.
        """
        file_name = self.find_file()
        if not file_name:
            file_path = os.path.join(self.directory,
                                     self._regex_file_pattern())
            raise FileNotFoundError(
                f"Weather File not present for {file_path}*")
        file_path = os.path.join(self.directory, file_name)
        return DelegatingTarget(file_path)

    def _regex_file_pattern(self) -> str:
        """
        Output is in format: '^.*YYYYMMDD*.*$'
        For example: '^.*20210422_03.*$'
        File Name Sample : weather.20160201.csv
        """
        run_date = self.date
        return f'^.*{run_date.strftime("%Y%m%d")}.*$'

    def find_file(self) -> str:
        """
        Using pattern matching to determine target file name.
        :raises FileNotFoundError
        """
        pattern = re.compile(self._regex_file_pattern())
        try:
            print(list(filter(pattern.match, os.listdir(self.directory))),
                  "in find_file()")
            return list(filter(pattern.match, os.listdir(self.directory)))[0]
        except IndexError:
            raise FileNotFoundError(
                f"Weather File not present for {pattern}*")


def get_max_temp_overall():
    """
    Get the highest ever temperature recorded
    """
    try:
        with open(
                "C:\\weather\\output\\overall\\weather.overall.csv",
                'r') as f:
            df = pd.read_csv(f)
        if df.size > 0:
            highest = df["ScreenTemperature"].loc[0]
            df_cleaned = df[["ObservationDate", "ScreenTemperature",
                             "Region"]]
    except OSError as e:
        return -99999, []
    return highest, df_cleaned


class WeatherTask(luigi.Task):
    """
    Task run once every day, one file per day
    dedupes the data and then gets the highest temp for that day
    any error rows are kept in //error directory 
    gets the record highest temp and compares with today's highest 
    if the highest temp is same as previous one, append
    if todays is higher than previous record high then overwrite file 	
    """
    date = luigi.DateParameter()

    def requires(self):
        return WeatherDataImportTask(self.date)

    def process_highest_temperature(self):
        df = self.load_raw_data_in_df()
        df.drop_duplicates(keep="first", inplace=True)
        df_err = df.loc[
            df.Region.isna() | df.ObservationDate.isna() | df.ScreenTemperature.isna()]
        if df_err.size:
            df_err.to_csv(f"C:\\weather\\input"
                          "\\error\\weather.error.{}.csv".format(
                self.date.strftime("%Y%m%d")))

        df_ok = df.loc[
            ~df.Region.isna() & ~df.ObservationDate.isna() & ~df.ScreenTemperature.isna()]

        if df_ok.size > 0:
            df_ok_enriched = df_ok[["ObservationDate", "ScreenTemperature",
                                    "Region"]]
            df_max = df_ok_enriched.loc[
                df_ok_enriched['ScreenTemperature'].max() == df_ok_enriched[
                    'ScreenTemperature']]
            df_max.to_csv(
                f"C:\\weather\\output\\daily"""
                "\\weather.daily.{}.csv".format(
                    self.date.strftime("%Y%m%d")), index=False)

            current_high, df_high = get_max_temp_overall()
            today_highest = df_max.iloc[([0][0])]["ScreenTemperature"]

            df_out = df_high
            if today_highest > current_high:
                df_out = df_max
                df_max.to_csv(
                    "C:\\weather\\output\\overall\\weather.overall.csv",
                    index=False)
            elif today_highest == current_high:
                df_high = df_high.append(df_max, ignore_index=True)
                df_high.drop_duplicates(keep="first", inplace=True)
                df_out = df_high
                df_high.to_csv(
                    "C:\\weather\\output\\overall\\weather.overall.csv",
                    index=False)
        return df_out

    def run(self):
        df = self.process_highest_temperature()
		if df.size > 0:
			#convert Dataframe to Apache Arrow Table
			table = pa.Table.from_pandas(df)
			pq.write_table(table,"C:\\weather\\input\\processed\\weather.overall.parquet.gzip",)
        self.output().write("Completed")

    def output(self):
    """
    Luigi needs an output task to know it has completed the days run
    if suppose the highest for the day is lesser than the record high
    then in that case we wont write to the "overall" file but we do not want the file 
	to be reporcessed. so, this file _temperature.tmp file is a "marker" to prevent rerun
    """
        return DelegatingTarget(
            os.path.join(
                "C:\\weather\\output\\done",
                '{0}_{1}_temperature.tmp'.format(
                    self.date,
                    "weather"
                ),
            ),
        )

    def load_raw_data_in_df(self) -> pd.DataFrame:
        """
        Read the data in the file into a dataframe for enriching and further
        processing
        """
        with open(self.input().path, 'r') as f:
            df = pd.read_csv(f)
        return df
