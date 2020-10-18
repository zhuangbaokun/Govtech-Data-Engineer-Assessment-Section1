# Govtech-Data-Engineer-Assessment

## Section 1: Data Pipelines

-The logical flow of the data pipeline are described in 2 parts - step 1 and step 2.

### Necessary Packages
1) time
2) pandas
3) schedule

### Step 1: Building the data pipeline function named datapipeline()

The datapipeline function is a function that basically performs the extraction, transformation and loading of data. This function contains many sub-function, and each sub-function is designed to perform on the the 4 listed processing task. The function consist of 6 main processing steps:
1) Import data from 'https://raw.githubusercontent.com/jaabberwocky/dataeng_test/master/dataset.csv' and save the data as df. Subsequently, this dataframe is fed into the other data processing functions.
2) The first data processing function is to split name into first_name and last_name. This function is named split_first_last_name().
-Assumption is made that the name is given in a format such that a name like "William Dixon", the first name will be "William" and the last name/surname will be "Dixon"
-To standardize, any tokens that are found in this list are removed ["PhD", "MD", "Ms.", "Mrs.", "Dr.", "Jr.", "Mr.", "DDS"," "]. This is to remove unnecessary designations eg. Mrs.
-Thereafter, perform the split
3) remove_prepended_zeros() removes prepended zeros from price column. Eg. "00090.00" will be converted to "90.00" by applying float to the entire price column.
4) To remove cases without name, we use the function remove_rows_without_name() to remove cases where the name is "" (empty string) or NaN.
5) The sub-function append_col_above_100(), applies an if else on the price column, so if price is strictly above 100 it outputs True, otherwise False. Thereafter, the result is appended to the dataframe as "above_100".
6) Lastly, after extracting the file from github and applying all the sub-functions, we will proceed to export the file into csv with the suffix "_update_(date of update)".

### Step 2: Scheduling the function to run everyday at a fixed time

1) pip install schedule
2) set the schedule to everyday at "01:01" and run the function datapipeline() in step 1
3) Create a while loop to kick start the scheduled function

### Output

-Result of the scheduled data pipeline output can be found in dataset_updated_20201016.csv
