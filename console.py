import click
from models import *
from toolbox.db_tools import *
from toolbox.os_tools import *


@click.command()
@click.argument('source_path', type=click.Path())
@click.argument('output_path', type=click.Path())
@click.option('-t', '--threads', type=int, default=4, help='Number of threads in several I/O-bound functions')
@click.option('-d', '--database', type=click.Path(), default='sqlite:///db.sqlite3', help='Database path')
def main(source_path, output_path, threads, database):
    """

    :param source_path: Path to zip folder with source data
    :param output_path: Path to directory where results should be saved
    :param threads: Number of threads in several I/O-bound functions
    :param database: Database path
    :return: None
    """

    unzipped = unzip_next_to(source_path)
    if not os.path.isdir(unzipped):
        click.echo(f"ERROR: {unzipped}")
        return False

    if threads > 9:
        threads = 9
        click.echo(f'You exceeded maximum recommended number of threads for this application. It is reset to 9.')
    click.echo(
        f'Execution started.\n'
        f'Source data will be retrieved from: {source_path}\n'
        f'Results will be saved in directory: {output_path}\n'
    )

    start = time.time()

    session = start_db_session(database)
    fill_table_from_csv(unzipped, session, Hotel)
    major_cities = find_major_cities(session, MajorCity)
    fill_addresses_for_major_cities(session, Hotel, major_cities, threads=threads)
    fill_major_cities_table_with_coordinates(session, Hotel, CityData, major_cities)
    fill_major_cities_table_with_temperatures(session, CityData)
    create_and_save_all_plots(session, CityData, output_path)
    write_temperature_analytics(session, CityData, output_path)
    write_from_db_to_files(output_path, session, Hotel, major_cities)

    execution_time = time.time() - start
    click.echo(f'Total execution time: {execution_time} seconds.')


if __name__ == '__main__':
    main()
