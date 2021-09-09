import os
import time

import click

from models import CityData, Hotel, MajorCity
from toolbox.db_tools import (create_and_save_all_plots,
                              fill_addresses_for_major_cities,
                              fill_major_cities_table,
                              fill_major_cities_table_with_coordinates,
                              fill_major_cities_table_with_temperatures,
                              fill_table_from_csv, find_major_cities,
                              start_db_session, write_from_db_to_files,
                              write_temperature_analytics)
from toolbox.os_tools import unzip_next_to
from web import configure_app


@click.command()
@click.argument('source_path', type=click.Path())
@click.argument('output_path', type=click.Path())
@click.option('-t', '--threads', type=int, default=4, help='Number of threads in several I/O-bound functions')
@click.option('-d', '--database', type=click.Path(), default='sqlite:///db.sqlite3', help='Database path')
def main(source_path, output_path, threads, database):
    """
    Main pipeline for processing hotels data.
    Provides moderate command line interface with required and optional arguments.
    Extracts valid records from csv files to database; finds cities with maximal number of hotels in each country;
    for hotels in those cities brings physical addresses; gets temperature information for 10 days in total
    starting from 5 days ago; creates temperature plots and other analytics; saves that analytics
    to provided output folder along with all hotels addresses.

    :param source_path: path to zip folder with source data, required argument
    :param output_path: path to directory where results should be saved, required argument
    :param threads: number of threads in several I/O-bound functions, optional argument
    :param database: database path, optional argument
    :return: None
    """
    click.echo(
        f'Execution started.\n'
        f'Source data will be retrieved from: {source_path}\n'
        f'Results will be saved in directory: {output_path}'
    )
    start = time.time()
    click.echo('Unzipping...')
    unzipped = unzip_next_to(source_path)
    if not os.path.isdir(unzipped):
        click.echo(f"ERROR: {unzipped}")
        return False
    if threads > 9:
        threads = 9
        click.echo('You exceeded maximum recommended number of threads for this application. It is reset to 9')

    session = start_db_session(database)
    click.echo('Cleaning data...')
    fill_table_from_csv(unzipped, session, Hotel)
    click.echo('Choosing cities with max number of hotels in country...')
    major_cities = find_major_cities(session, MajorCity)
    fill_major_cities_table(session, MajorCity, major_cities)
    click.echo('Fetching addresses for hotels located in these cities...')
    fill_addresses_for_major_cities(session, Hotel, major_cities, threads=threads)
    click.echo('Calculating cities centers coordinates...')
    fill_major_cities_table_with_coordinates(session, Hotel, CityData, major_cities)
    click.echo('Fetching weather statistics for cities centers...')
    fill_major_cities_table_with_temperatures(session, CityData, threads=threads)
    click.echo('Creating and saving temperature plots for cities centers...')
    create_and_save_all_plots(session, CityData, output_path)
    click.echo('Creating and saving cities temperature analytics...')
    write_temperature_analytics(session, CityData, output_path)
    click.echo('Saving cities hotels data to csv files...')
    write_from_db_to_files(output_path, session, Hotel, major_cities)

    execution_time = time.time() - start
    click.echo(f'Total execution time: {execution_time} seconds.')

    web_app = configure_app(db_path=database)
    web_app.run()


if __name__ == '__main__':
    main()
