import unittest
import os

from work_with_testlib import (get_testlib,
                               get_testlib_answer,
                               delete_testlib)
from generate_solution import generate_solution_code
from update_filesystem import refresh_filesystem

TIMEOUT_PLATFORM = 60

def call_testlib(folder, get_log=False):
    if 'test.py' not in next(os.walk(folder))[2]:
        return
    
    generate_solution_code(folder)
    
    answer = get_testlib_answer(
        'solution.py',
        'author.py',
        'test.py',
        'hidden_precode.py',
        folder,
        TIMEOUT_PLATFORM,
        get_log
    )
    
    os.remove(f'{folder}/solution.py')
    os.remove(f'{folder}/hidden_precode.py')
    refresh_filesystem(folder)
    
    if not isinstance(answer, dict):
        print(f"[{folder}]", answer)
        assert False, answer
    if 'solved' not in answer:
        print(f"[{folder}]", answer)
        assert False, answer
    print(f"[{folder}]", answer['solved'])
    assert answer['solved'], f'Not solve {folder}, error: ' + answer['error']['id']


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