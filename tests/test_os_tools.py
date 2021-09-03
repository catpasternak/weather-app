import os
import shutil
from toolbox.os_tools import path_to_, unzip_next_to, create_city_folder


def test_path_to_func_result_created_from_root():
    path_parts = ('toolbox', 'os_tools.py')
    path = path_to_(*path_parts)
    assert os.path.exists(path)


def test_unzip_func_return_message_when_path_not_exist():
    provided_source_path = 'elephants'
    result = unzip_next_to(provided_source_path)
    assert result == f"Directory {path_to_(provided_source_path)} doesn't exist. Please provide existing directory"


def test_unzip_func_return_message_when_path_not_zip_or_directory():
    provided_source_path = 'tests/test_data/text.txt'
    result = unzip_next_to(provided_source_path)
    assert result == f"" \
                     f"Directory {path_to_(provided_source_path)} is not a valid directory. " \
                     f"Please provide zip file or directory"


def test_unzip_func_return_message_when_path_directory_contains_no_csv():
    provided_source_path = 'tests/test_data/no_csv_here'
    result = unzip_next_to(provided_source_path)
    assert result == f'{provided_source_path} is neither a zip file, nor a directory with csv files inside'


def test_unzip_func_return_new_directory_with_csv_files_when_source_directory_not_zip_but_contains_csv():
    provided_source_path = 'tests/test_data/csv_here'
    result = unzip_next_to(provided_source_path)
    assert result == os.path.join(os.path.dirname(__file__), 'test_data/hotels_unzipped')
    assert os.listdir(os.path.join(os.path.dirname(__file__), 'test_data/hotels_unzipped'))


def test_unzip_func_return_new_directory_with_csv_files_when_source_directory_is_zip_file():
    provided_source_path = 'tests/test_data/csv_here/test_hotels.zip'
    result = unzip_next_to(provided_source_path)
    assert result == os.path.join(os.path.dirname(__file__), 'test_data/csv_here/hotels_unzipped')
    assert os.listdir(os.path.join(os.path.dirname(__file__), 'test_data/csv_here/hotels_unzipped')) == ['source_file1.csv']


def test_create_city_folder_func_creates_folders_correctly():
    output_path = path_to_('tests/test_data/output_dir')
    country = 'AT'
    city = 'Vienna'
    path = path_to_(output_path, country, city)
    if os.path.exists(path):
        shutil.rmtree(path)
    create_city_folder(output_path, country, city)
    assert os.path.exists(path)
