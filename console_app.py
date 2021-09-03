import time

import click
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
OUTPUT_PATH = 'output_data'


@click.command()
@click.argument('source_path', type=click.Path(exists=True))
@click.argument('output_path', type=click.Path())
@click.option('-t', '--threads', type=int, default=4)
def main(source_path, output_path, threads):
    start = time.time()

    click.echo(f'src_path argument: {source_path}, destination path argument: {output_path}')

    new_dir = unzip_next_to(source_path)

    session = start_db_session('sqlite:///db.sqlite3')

    fill_table_from_csv(new_dir, session, Hotel)

    major_cities = find_cities(session, MajorCity)

    fill_addresses_for_major_cities(session, Hotel, major_cities, threads=threads)

    fill_table_with_coordinates(session, Hotel, Temperature, major_cities)

    fill_temp_min_max(session, Temperature)

    create_and_save_all_plots(session, Temperature, output_path)

    write_temperature_analytics(session, Temperature, output_path)

    write_from_db_to_files(output_path, session, Hotel, major_cities)

    execution_time = time.time() - start

    click.echo(f'Total execution time: {execution_time} seconds.')


if __name__ == '__main__':
    main()





"""
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

fill_temp_min_max(session, Temperature)

########### Create plots ##############################################################################################

# create_and_save_all_plots(session, Temperature, OUTPUT_PATH)

########### Analytics #################################################################################################

# write_temperature_analytics(session, Temperature, OUTPUT_PATH)

############### Write CSV #############################################################################################

write_from_db_to_files(OUTPUT_PATH, session, Hotel, major_cities)


"""