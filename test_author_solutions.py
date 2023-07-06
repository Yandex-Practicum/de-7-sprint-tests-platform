import unittest
import os

from work_with_testlib import (get_testlib,
                               get_testlib_answer,
                               delete_testlib)
from generate_solution import generate_solution_code

TIMEOUT_PLATFORM = 20


class RegressionTest(unittest.TestCase):
    def testlib_run(self):
        try:
            folder = self.folder
        except:
            return
        
        with self.subTest(folder=folder):
            generate_solution_code(folder)
            
            # TODO: Переделать вывод, добавить вердикт сразу

            answer = get_testlib_answer(
                'solution.py',
                'author.py',
                'test.py',
                folder,
                TIMEOUT_PLATFORM,
            )
            
            os.remove(f'{folder}/solution.py')
            # TODO: Обнулить файловую систему в докере
            assert answer['solved'], f'Not solve {folder}'
            print(f"[{folder}]", answer['solved'])

            
    def test_run(self):
        get_testlib()
        for folder in os.listdir(path='.'):
            if folder[:2] != 'de':
                continue
            self.folder = folder
            self.testlib_run()

        delete_testlib()

if __name__ == "__main__":
    unittest.main()
    # print(os.lstat('work_with_testlib.py'))