import os
import unittest
from datetime import datetime
from unittest.mock import patch

import luigi
import pandas as pd
from pandas.testing import assert_frame_equal

from weather.weather_tasks import (
    WeatherTask,
)

CURRENT_DIR = os.getcwd()
test_file = os.path.join(CURRENT_DIR, 'weather_tasks.csv')
test_file_1 = os.path.join(CURRENT_DIR, 'weather_tasks_1.csv')
test_file_2 = os.path.join(CURRENT_DIR, 'weather_tasks_2.csv')
output_file = os.path.join(CURRENT_DIR, 'output.csv')


def create_df1():
    expected_columns = ["ObservationDate", "ScreenTemperature",
                        "Region"]
    test_data_after_loaded_in_df = [
        ['2016-02-01T00:00:00', 2.1, 'Orkney & Shetland']
    ]
    df2 = pd.DataFrame(test_data_after_loaded_in_df,
                       columns=expected_columns)
    return tuple((2.1, df2))


def create_df2():
    expected_columns = ["ObservationDate", "ScreenTemperature",
                        "Region"]
    test_data_after_loaded_in_df = [
        ['2016-02-02T00:00:00', 3.0, 'Highlands']
    ]
    df2 = pd.DataFrame(test_data_after_loaded_in_df,
                       columns=expected_columns)
    return tuple((3.0, df2))


class TestWeatherTask(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.date = datetime(2016, 2, 1, 00)
        cls.task = WeatherTask(cls.date)
        cls.expected_columns = ["ObservationDate", "ScreenTemperature",
                                "Region"]
        cls.test_data_after_loaded_in_df = [
            ['2016-02-01T00:00:00', 2.1, 'Orkney & Shetland']
        ]
        cls.df2 = pd.DataFrame(cls.test_data_after_loaded_in_df,
                               columns=cls.expected_columns)

    @patch(
        'weather.weather_tasks.get_max_temp_overall',
        return_value=(-99999, []))
    @patch(
        'weather.weather_tasks.WeatherTask.input',
        return_value=luigi.LocalTarget(test_file))
   def test_intial_load(self, target_mock, current_temp):
        target_mock.path = test_file
        curr = current_temp
        self.test_data_after_loaded_in_df = [
            ['2016-02-01T00:00:00', 2.1, 'Orkney & Shetland']
        ]
        expected_data = self.test_data_after_loaded_in_df
        expected_number_of_records = 1
        expected_df = pd.DataFrame(expected_data, columns=self.expected_columns)

        actual_df = self.task.process_highest_temperature()
        actual_number_of_records = len(actual_df.index)
        assert_frame_equal(expected_df, actual_df)
        self.assertEqual(expected_number_of_records, actual_number_of_records)

    @patch(
        'weather.weather_tasks.get_max_temp_overall',
        return_value=create_df1())
    @patch(
        'weather.weather_tasks.WeatherTask.input',
        return_value=luigi.LocalTarget(test_file_1))
    def test_data_overwritten_when_higher_temp_found(self, target_mock,
                                                     current_temp):
        target_mock.path = test_file
        curr = current_temp
        self.test_data_after_loaded_in_df = [
            ['2016-02-02T00:00:00', 3.0, 'Highlands']
        ]
        expected_data = self.test_data_after_loaded_in_df
        expected_number_of_records = 1
        expected_df = pd.DataFrame(expected_data, columns=self.expected_columns)

        actual_df = self.task.process_highest_temperature()
        actual_number_of_records = len(actual_df.index)
        assert_frame_equal(expected_df, actual_df)
        self.assertEqual(expected_number_of_records, actual_number_of_records)

    @patch(
        'weather.weather_tasks.get_max_temp_overall',
        return_value=create_df2())
    @patch(
        'weather.weather_tasks.WeatherTask.input',
        return_value=luigi.LocalTarget(test_file_2))
    def test_data_appended_when_matching_temp_found(self, target_mock,
                                                    current_temp):
        target_mock.path = test_file
        curr = current_temp
        print(curr[0])
        self.test_data_after_loaded_in_df = [
            ['2016-02-02T00:00:00', 3.0, 'Highlands'],
            ['2016-02-03T00:00:00', 3.0, 'Lowlands']
        ]
        expected_data = self.test_data_after_loaded_in_df
        expected_number_of_records = 2
        expected_df = pd.DataFrame(expected_data, columns=self.expected_columns)

        actual_df = self.task.process_highest_temperature()
        actual_number_of_records = len(actual_df.index)
        assert_frame_equal(expected_df, actual_df)
        self.assertEqual(expected_number_of_records, actual_number_of_records)


if __name__ == '__main__':
    unittest.main()