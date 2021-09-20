"""
The data sets used are from : 
https://www.citibikenyc.com/system-data
https://www.kaggle.com/muthuj7/weather-dataset
"""
import pandas as pd
from checks import CheckTimeStamps


BIKE_DATA_SOURCE = "2014-02 - Citi Bike trip data.csv"
WEATHER_DATA_SOURCE = "weatherHistory.csv"


class DataSource:
    """
    Data source class which allows for different dataset checks to the data_source
    This approach would preserve the open-closed principle.
    Base class which could be re-used to load different data_sources.
    """

    def __init__(self, data_source):
        self.data_name = data_source
        self.data_source = self.load(self.data_name)
        self._checks = []

    def __repr__(self):
        return f"<DS: {self.data_name} with Checks: {' | '.join([str(ch) for ch in self._checks])}>"

    @property
    def checks(self):
        """
        A list of all the checks that will be performed on the data set
        """
        return self._checks

    def add_check(self, check):
        """
        Adds a check to the checks lists that will be performed on the data source
        """
        self._checks.append(check)

    def remove_check(self, check):
        """
        Removes "check" from the cleaners list that will be applied to the dataset.
        """
        self._checks.remove(check)

    def check_data(self):
        """
        Execute all check objects on the data source.
        """
        for check in self._checks:
            check.apply(self.data_source)

    def merge_with(self, other_source):
        """
        Merges the data source with another data source. Depending on the type of the data source
        each child could define a specific behavior depending on the type of the expected data source.

        This method should be defined within the child classses dor the specific data source.
        """
        raise NotImplementedError

    def load(self, data_source):
        """
        Load the data set. Needs to be implemented for the specific dataset
        """
        raise NotImplementedError


class SQLSource(DataSource):
    """
    Facility to work with SQL data sources.
    """

    def merge_with(self, other_source):
        """
        """

    def load(self, data_source):
        """
        Load the sql dataset.

        data_source: string - name of the datafile
        """
        pass


class CSVSource(DataSource):
    """
    Facility to work with CSV data sources.
    """

    def merge_with(self, other_source):
        """
        Merges two csv data sets.
        """
        # Does not override the initial data source but rather returns a new one
        return pd.merge(self.data_source, other_source.data_source)

    def load(self, data_source):
        """
        Load the csv dataset.

        data_source: string - name of the datafile
        """
        return pd.read_csv(data_source)


if __name__ == "__main__":
    # Initialize the data sets
    bikes = CSVSource(BIKE_DATA_SOURCE)
    weather = CSVSource(WEATHER_DATA_SOURCE)
    # Add the needed checks for the corresponding columns and execute them
    bikes.add_check(CheckTimeStamps("starttime", "stoptime"))
    weather.add_check(CheckTimeStamps("Formatted Date"))
    bikes.check_data()
    weather.check_data()
    # Since the weather timestamps are only on round hours, I had to add a new column in the bikes dataset
    # with rounded datetime so it could be matched during the merge
    bikes.data_source["Formatted Date"] = bikes.data_source["starttime"].astype(
        "datetime64[h]")
    # The datatype in the second dataset was not the correct type which is why this was needed.
    weather.data_source["Formatted Date"] = weather.data_source["Formatted Date"].astype(
        "datetime64[h]")

    merged = bikes.merge_with(weather)
    # Export to a new datafile for further visualizations
    merged.to_csv("output.csv")
