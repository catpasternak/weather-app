from zipfile import ZipFile, is_zipfile
import os
import shutil


def path_to_(*path_parts: str):
    cwd = os.path.dirname(__file__)
    root_dir = cwd.rsplit('/', 1)[0]
    return os.path.join(root_dir, *path_parts)


def unzip_next_to(file_path):
    """
    Unzips files from given zip file path to new folder created next to its directory.
    Considered situations:
    - path doesn't exist or not a directory (i.e. file): return message
    - path is not a zip file: if it contains csv file - ok, copy them to new folder, else - return message,
    in case that it has the same name as folder prepared for unzipping - leave it still.
    :param file_path: path to zip folder with source data
    :return: str
    """
    if not os.path.exists(path_to_(file_path)):
        return f"Directory {path_to_(file_path)} doesn't exist. Please provide existing directory"
    if not any((os.path.isdir(path_to_(file_path)), is_zipfile(path_to_(file_path)))):
        return f"Directory {path_to_(file_path)} is not a valid directory. Please provide zip file or directory"
    new_dir = os.path.join(os.path.dirname(path_to_(file_path)), 'hotels_unzipped')
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
    if not is_zipfile(file_path):
        csv_files = [f for f in os.listdir(path_to_(file_path)) if f.endswith('.csv')]
        if not csv_files:
            return f'{file_path} is neither a zip file, nor a directory with csv files inside'
        if new_dir != path_to_(file_path):
            for file_ in csv_files:
                shutil.copy(path_to_(file_path, file_), new_dir)
        return new_dir
    with ZipFile(file_path, 'r') as zipObj:
        if not os.listdir(new_dir):
            zipObj.extractall(new_dir)
    return new_dir


def create_city_folder(output_path, country, city):
    if not os.path.exists(path_to_(output_path)):
        os.mkdir(path_to_(output_path))
    if not os.path.exists(path_to_(output_path, country)):
        os.mkdir(path_to_(output_path, country))
    if not os.path.exists(path_to_(output_path, country, city)):
        os.mkdir(path_to_(output_path, country, city))
    return path_to_(output_path, country, city)
