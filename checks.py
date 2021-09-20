"""
Module containing all checks that could be applied on a data source. Currently they are raising an exception but 
could be extended to actually clean the data and be applied to different types of data sets.
"""
from collections import defaultdict
from datetime import datetime
import pandas as pd


class DataException(Exception):
    """
    Custom exception for data checks. 
    """


class ValidDataException(DataException):
    """
    Exception for any invalid data input.
    """


class Check:
    """
    Base class for the different cleaner types which can be attached to the different data sets. 
    The columns passed in the constructor are the ones on which this check will be performed.
    """

    def __init__(self, *columns):
        self.columns = columns

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def apply(self, data_source):
        """
        Apply method which needs to be implemented by every cleaner child. 
        """
        raise NotImplementedError


class CheckTimeStamps(Check):
    """
    Check that takes care of the date/timestamp fields of a CSV dataset.
    The columns passed in the constructor are the ones on which this check will be performed.
    """

    def apply(self, data_source):
        """
        Checks the date/timestamp in the dataset and formats it properly
        so they are consistent throughout the datasets.
        """
        for column in self.columns:
            data_source[column] = data_source[column].apply(
                lambda x: pd.Timestamp(x).strftime('%Y-%m-%d %X'))


# ============== Possible additional dataset checks =====================
class CheckBlank(Check):
    """
    Check for blank columns that don't provide any value in the dataset.
    """

    def apply(self, data_source):
        """
        Apply the check to the columns of the 'data_source' passed in the constructor. 
        Raises an exception with a list of columns that are empty.
        """
        empty_cols = []
        for col in self.columns:
            # This seems to be the fastest way compared to empty and len
            # https://stackoverflow.com/questions/24652417/how-to-check-if-pandas-series-is-empty
            if not len(data_source[col].index):
                empty_cols.append(col)
        if empty_cols:
            raise DataException("Empty columns found", empty_cols)


class CheckGibberrish(Check):
    """
    Checks for giberrish in the passed columns in the dataset.

    The gibberish list needs to be passed in during initialization.
    """
    # The check could be applied agains a list of common gibberish inputs
    # e.g ??, ... , aaaaa, fefef etc.
    # that could be defined in a separate table/database so it could be
    # easily extended with additional inputs
    # For the current usecase I'll use a simple list with a few values.

    def __init__(self, *columns, giberrish_list=[]):
        super().__init__(*columns)
        self.gibberish_list = giberrish_list
        self._found_issues = defaultdict(str)

    def apply(self, data_source):
        """
        Apply the check for each item in the passed columns.
        """
        for col in self.columns:
            data_source[col].apply(lambda x: self._check_gibberish(col, x))
        if self._found_issues:
            raise ValidDataException(
                "Gibberish inputs found", self._found_issues)

    def _check_gibberish(self, col, item):
        if item in self.gibberish_list:
            self._found_issues[col] = item


class CheckDuplicate(Check):
    """
    Checks for duplicate data within the dataset. This check could be done to data columns
    which should have unique values (e.g id columns)
    """

    def apply(self, data_source):
        """
        Apply the check to the columns from the constructor. 
        Raises an exception with the columns that don't have unique values. 
        """
        cols_with_duplicate_values = []
        for col in self.columns:
            if not data_source[col].is_unique:
                cols_with_duplicate_values.append(col)
        if cols_with_duplicate_values:
            raise ValidDataException(
                "Columns containing duplicate values", cols_with_duplicate_values)


class CheckDataValidity(Check):
    """
    Checks that the data provided is valid agains a list of possible values
    e.g: There are no non-existing countries etc. 

    The constuctor also accepts an iterable of possible values agains which 
    the values of the columns will be checked.
    """

    def __init__(self, *columns, possible_values=[]):
        super().__init__(*columns)
        self._possible_values = possible_values
        self._found_issues = defaultdict(set)

    def apply(self, data_source):
        """
        Apply the validity check to all passed columns at the data_source.
        """
        for col in self.columns:
            data_source[col].apply(lambda x: self._check_data_inputs(col, x))

        if self._found_issues:
            raise ValidDataException(
                "Incorrect values found", self._found_issues)

    def _check_data_inputs(self, col, item):
        if item not in self._possible_values:
            self._found_issues[col].add(item)


class CheckFutureDates(Check):
    """
    Checks if the passed columns with timestamps are valid and the values do not show future dates.
    """

    def __init__(self, *columns):
        super().__init__(*columns)
        self._found_issues = set()
        self._current_time = pd.to_datetime(datetime.now())

    def apply(self, data_source):

        for col in self.columns:
            data_source[col].apply(lambda x: self._check_time(col, x))

        if self._found_issues:
            raise ValidDataException(
                "Future date found in column(s)", self._found_issues)

    def _check_time(self, col, item):
        if self._current_time < pd.to_datetime(item):
            self._found_issues.add(col)


class CheckNumWithinRange(Check):
    """
    Checks that all values of the passed columns is within a passed range:
    """

    def __init__(self, *columns, range_start=0, range_end=0):
        super().__init__(*columns)
        self.range_start = range_start
        self.range_end = range_end
        self._found_issues = []

    def apply(self, data_source):
        """
        Apply the check for each item in the passed columns.
        """
        # Sanity check if the passed ranges are invalid
        if self.range_start > self.range_end:
            raise ValueError(
                f"The starting point ({self.range_start}) should be less than the ending point ({self.range_end}).")

        for col in self.columns:
            if data_source[col].min() < self.range_start or data_source[col].max() > self.range_end:
                self._found_issues.append(col)
        if self._found_issues:
            raise ValidDataException(
                "Inputs outside the specified ranges found", self._found_issues)


class CheckDataConsistency(Check):
    """
    Checks that the data format across passed data columns is consistent.
    (e.g longtitude and latitude should have the same type of value)
    """

    def apply(self, data_source):
        """
        Apply the check to all the data columns passed in the constructor.
        Raises an exception if any of the columns is not of the same type
        """
        # No need to check for consistency if less than 2 columns were passed
        if len(self.columns) < 2:
            return

        # @TODO: This could be improved to at least use a dictionary so we know which column
        # is with the different type from the rest
        collected_types = {data_source[col].dtype for col in self.columns}
        # If the set contains more than 1 elements then the exception should be triggered
        if len(collected_types) > 1:
            raise ValidDataException(
                "The passed columns are not of the same type, the collected types are:", collected_types)


class CheckDataRelevancy(Check):
    """
    Checks that the fields in the data set are relevant to the specific usecase.
    e.g the columns in the data set are compared to a iterable with expected columns
    (for example with columns from similar datasets).
    The construcor also expects an iterable containing the expected columns.
    """

    def __init__(self, *columns, expected_columns=[]):
        super().__init__(*columns)
        self.expected_columns = expected_columns

    def apply(self, data_source):
        """
        Applies the column check to the entire data set comparing the 
        columns to an iterable of 'expected_columns'
        Raises an exception if such columns are present
        """
        # This could be refined with some regular expressions for example
        # but my general idea is something like this
        unexpected_cols = []
        for col in data_source.columns:
            if col not in self.expected_columns:
                unexpected_cols.append(col)
        # If there is no unexpected column, the check passes, otherwise raises an Exception
        # showing the columns that need attention
        if unexpected_cols:
            raise DataException(
                "Columns with irrelevant information found", unexpected_cols)


class CheckIsNaN(Check):
    """
    Checks if there are NaN values contained within the passed in the constructor columns.
    If there are any, an exception is raised with the columns listed so, any action could be taken.
    """

    def apply(self, data_source):
        """
        Applies the check to the passed data_source.
        """
        cols_with_nan = []
        for column in self.columns:
            if data_source[column].isnull().values.any():
                cols_with_nan.append(column)
        if cols_with_nan:
            raise ValidDataException(
                "Columns containing NaN values", cols_with_nan)

            # ============== Possible additional dataset checks =====================
