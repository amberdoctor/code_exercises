# csv_etl

## Run csv_etl.py

`code_sample.py` is designed to run from the command line by entering `python3.6 csv_etl.py {path_to_read} {path_to_write}`.

Where {path_to_read} is the path and filename of the csv that will be read.
Where {path_to_write} is the path to the directory where files will be created.


## Run tests

You can run the tests by navigating to the directory at the command line and entering `python3.6 tests.py`.


## Exercise Description
1. Download sample CSV file from https://goo.gl/i4hCHy

2. Write command line tool in Python (2.7 or 3.*) which takes as parameter path to above CSV file and path to the output directory.

3. Let the script do what follows:
..- Read above CSV file
..- Print 10 the most often repeated values from "Name" column
..- Remove all rows with "Status" value different than "Active"
..- Group rows by "Type" column
..- Reverse columns order
..- Save (to the output directory) each type in a separate CSV file, named after its type

4. Let the solution be tested


## Notes
I am expressing the problem as a directed acyclic graph, so I can easily extend the solution to include new computations.

I've left a few to do comments in the document that express items that I would want to get business clarification on or would tackle if I had more time.