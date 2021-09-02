from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import *
from toolbox import *
"""
click parameters:
- source path incl zip file name
- destination path for results
- number of threads
- ...
"""
SOURCE_PATH_CLICK = 'source_data/hotels.zip'

########## Unzip ######################################################################################################

new_dir = unzip_next_to(SOURCE_PATH_CLICK)  # OK, done

########## Read and clean data, start and fill db #####################################################################

engine = create_engine('sqlite:///db.sqlite3')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

hotels_coord_list = fill_table_from_csv(new_dir, session, Hotel)  # OK, done

########## Find major cities ##########################################################################################

major_cities = find_cities(session, MajorCity)

########### Fill addresses in db for major cities #####################################################################

fill_addresses_for_major_cities(session, Hotel, major_cities, threads=4)

########### Find major city centers and fill the table ################################################################

fill_table_with_coordinates(session, Hotel, Temperature, major_cities)

########### Get temperatures ##########################################################################################

fill_temperatures(session, Temperature)

