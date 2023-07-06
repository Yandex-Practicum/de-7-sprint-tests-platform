import unittest
import os

from work_with_testlib import (get_testlib,
                               get_testlib_answer,
                               delete_testlib)
from generate_solution import generate_solution_code

TIMEOUT_PLATFORM = 20

def call_testlib(folder, get_log=False):
    generate_solution_code(folder)
    answer = get_testlib_answer(
        'solution.py',
        'author.py',
        'test.py',
        folder,
        TIMEOUT_PLATFORM,
        get_log
    )
    os.remove(f'{folder}/solution.py')
    # TODO: Обнулить файловую систему в докере
    
    assert answer['solved'], f'Not solve {folder}, error: ' + answer['error']['id']
    print(f"[{folder}]", answer['solved'])


class RegressionTest(unittest.TestCase):           
    def test_run(self):
        get_testlib()
        for folder in os.listdir(path='.'):
            if folder[:2] != 'de':
                continue
            with self.subTest(folder=folder):
                call_testlib(folder)

        delete_testlib()

if __name__ == "__main__":
    unittest.main()
    # print(os.lstat('work_with_testlib.py'))