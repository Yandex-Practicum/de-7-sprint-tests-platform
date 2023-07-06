import os
import subprocess
import json
from zipfile import ZipFile

EXCEPT_FILES = [
    'test_author_solutions.py', 
    'work_with_testlib.py',
    'generate_solution.py',
    'update_filesystem.py',
    ]

def get_testlib():
    zf = ZipFile('/testlibs/ast_testlib_stable.py', 'r')
    zf.extractall('.')


def get_testlib_answer(user_py, author_py, test_py, folder, timeout=60, get_log=False):
    cmd = [
        'python3', '__main__.py',
        '-u', f'{folder}/{user_py}',
        '-a', f'{folder}/{author_py}',
        '-t', f'{folder}/{test_py}',
        '--keep', '--no-slack',
        ]
    if get_log:
        cmd.append('--log-data')
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, timeout=timeout)
        output = result.stdout.decode('utf-8')
    except subprocess.TimeoutExpired:
        output = f'TimeoutExpired in task {folder}'
        
    try:
        return json.loads(output)
    except Exception:
        assert False, output


def delete_testlib():
    files = next(os.walk('.'))[2]
    for f in files:
        if f[-3:] == '.py' and f not in EXCEPT_FILES:
            os.remove(f'./{f}')