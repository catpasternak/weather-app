import os
import pathlib
import shutil
from zipfile import ZipFile, BadZipFile, is_zipfile


def path_to_(*path_parts):
    """
    Path to directory or file from project root directory
    :param path_parts: directories on the way and destination directory or file
    :type path_parts: List[str]
    :rtype: Path
    """
    cwd = os.path.dirname(__file__)
    root_dir = cwd.rsplit('/', 1)[0]
    return os.path.join(root_dir, *path_parts)


def unzip_next_to(file_path):
    """
    Unzips files from given zip file path to new folder created next to it.
    Handled situations:
    - path doesn't exist or not a directory (i.e. file): return message
    - path is not a zip file: if it contains csv file - ok, copy them to new folder, else - return message,
    in case that it has the same name as folder prepared for unzipping - leave it still.
    :param file_path: path to zip folder with source data
    :return: path to directory with unzipped csv files
    :rtype: str
    """
    if not os.path.exists(path_to_(file_path)):
        return f"Directory {path_to_(file_path)} doesn't exist. Please provide existing directory"
    new_dir = os.path.join(os.path.dirname(path_to_(file_path)), 'hotels_unzipped')
    pathlib.Path(new_dir).mkdir(exist_ok=True)
    try:
        with ZipFile(file_path, 'r') as zipObj:
            if not os.listdir(new_dir):
                zipObj.extractall(new_dir)
    except IsADirectoryError:
        csv_files = [f for f in os.listdir(path_to_(file_path)) if f.endswith('.csv')]
        if not csv_files:
            return f'{file_path} is neither a zip file, nor a directory with csv files inside'
        if new_dir != path_to_(file_path):
            for file_ in csv_files:
                shutil.copy(path_to_(file_path, file_), new_dir)
    except BadZipFile:
        return f"Directory {path_to_(file_path)} is not a zip file. Please provide zip file or directory"
    return new_dir


def create_city_folder(output_path, country, city):
    """
    Creates folders if not existent according to following structure: {output_folder}/{country}/{city}
    :param output_path: path to output folder
    :param country: country Alpha-2 code
    :param city: city name
    :return: full path to city folder
    :rtype: Path
    """
    new_dir = path_to_(output_path, country, city)
    pathlib.Path(new_dir).mkdir(parents=True, exist_ok=True)
    return new_dir
