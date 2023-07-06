import os
import shutil
import yaml

def delete_files_or_directories(path):
    if path is None:
        return
    if path[-1] == "/":
        shutil.rmtree(path)
    else:
        os.remove(path)

def refresh_files_or_directory(path):
    if path is None:
        return
    source = path.replace('user', 'user_copy')
    delete_files_or_directories(path)
    if path[-1] == "/":
        shutil.copytree(source, path)
    else:
        shutil.copyfile(source, path)

def refresh_filesystem(folder):
    if 'config.yaml' not in next(os.walk(folder))[2]:
        return
    
    with open(f'{folder}/config.yaml', "r+", encoding='utf-8') as f:
        config = yaml.safe_load(f)
    path_to_delete = config['path_to_delete'] if config['path_to_delete'] is not None else []
    path_to_refresh = config['path_to_refresh'] if config['path_to_refresh'] is not None else []
    
    for path in path_to_delete:
        delete_files_or_directories(path)
    for path in path_to_refresh:
        refresh_files_or_directory(path)
