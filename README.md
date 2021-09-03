This is a command line interface application
that analyses weather for hotels provided in source directory
(that is first command line argument).

Results will be written to output folder (that is second argument).

Third argument is number of threads that will be used
for requesting data from geocoding and weather services.
Thread number argument is optional, has a default value of 4
and should be used with flag '--threads' or '-t'.

There is also forth optional argument with flag '--database' or '-d',
that sets path to database system and file location. It is currently set to
'sqlite:///db.sqlite3'. Current configuration is strongly recommended
since application was tested only with python built-in SQLite database engine.

Example:

>>> python3 console.py source_data/hotels.zip output_data --threads 4

All required dependencies are present in requirements.txt