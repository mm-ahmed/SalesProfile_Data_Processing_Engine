****************************************
THE DATA PROCESSING ENGINE REQUIREMENTS:
****************************************
1 - Given is a TSV file (sales_data.tsv) with dummy sales data in the 'StaticData' folder. Each row contains a transaction
    record that describes how many units of a product have been sold at a specific store at a specific time.

2 - The sales profiles need to be computed for a subset of stores and write those sales profiles to a single JSON file in the
    'OutputFiles' folder.
    2.1 -   TABLEAU PUBLICH SERVER: The transformed file is also saved as csv and connected to the Tableau for dashboarding purpose.
            Public link to access the deployed dashboard: https://public.tableau.com/authoring/SalesProfile_16829394013360/SalesProfileDashboard#1
            (A screenshot of the dashboard has been captured for a quick glance.)
    2.2 -   Power BI: The same csv version is also stored into the folder 'OutputFiles/PowerBI' where he generated csv has a live
            connection with the PowerBI file 'sales_profile_power_bi_file.pbix' in the same folder. (A screenshot of the dashboard has
            been captured for a quick glance.)

3 - A sales profile is computed by summing up the units sold for each product in each store and dividing by the total sum of units
    sold per store. These normalised unit sales must sum up to one (per store). This allows the comparison of stores independent
    of their total sales. For instance, the example above reveals that store 1 sells relatively more coffee than store 3, while
    store 3 sells more doughnuts.


*********************
TRANSFORMATION RULES:
*********************
1 - There are invalid transactions with negative units that need to be filtered out.

2 - There are different “spellings” for the same product that need to be unified, e.g. “coffee-large”, “coffee_large” and “coffee large”.

3 - The 'store_id' value(s) as input parameter must be integer, and must exist in the source data.

4 - Using PyTest perform the integration testing with 100% coverage.

5 - RECOMMENDATION: Use Pandas to compute the sales profiles and write the JSON output, once the data has been aggregated via PySpark.


***********************************
CALCULATING SALES PROFILE BY STORE:
***********************************
1 - The Process:
    The purpose of the 'SalesProfile' process is to compute sales profile for a subset of stores. The process makes use of various data structures,
    which primarily include, PySpark DataFrame, Pandas DataFrame, Dict, List.

2 - Input Instructions:
    Depending upon the process requirements, it takes 'integers' as input parameter values, where each integer value corresponds to the 'STORE ID' in
    the source dataset. If the input parameter value(s) corresponds to the one(s) in the source dataset, then the process will progress to perform the
    tranformation and computation on the store specified subset based on the task requirements. You may try the follwoing command(s) to run the process:

        python main.py 0 1 2 3

3 - Files:
    The folder 'Output Files' contain two files, named: 'sales_profile_by_store.json' and 'test_sales_profile_by_store.json', where the former file contains
    the computed resultset, and the later on is used by the 'tests.py' module.

4 - Test:
    The process has been tested with 100% coverage on all the modules and have passed all the tests. The testing process can be found in the module named: tests.py
    and the following command runs all the tests and provides the coverage report on Terminal.

        pytest --cov-report term-missing --cov=. tests.py

***************
CODE STRUCTURE:
***************
1 - The file 'main.py' initiates the transformation components.

2 - The file 'utils.py' provides utility functions to all the transformation components.

3 - The file 'transformation.py' is the process engine to meet most of the engine's requirements and the transformation rules.

4 - The file 'tests.py' performs the integration testing.

5 - The file 'static_data.py' in the StaticData folder keeps the constant or static values to be used across the engine's components.

6 - The file 'sales_data.tsv' in the StaticData folder is the dummy source data.

7 - The OutputFiles folder contains the generated output by the process in JSON as well as in csv (under Tableau and PowerBI subfolder).
