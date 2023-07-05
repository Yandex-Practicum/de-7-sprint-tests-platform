import unittest
import os

from work_with_testlib import (get_testlib,
                               get_testlib_answer,
                               delete_testlib)


TIMEOUT_PLATFORM = 20


class RegressionTest(unittest.TestCase):   
    def test_run(self):
        get_testlib()
        for folder in os.listdir(path='.'):
            if folder[:2] != 'de':
                continue
            with self.subTest(folder=folder):
                # TODO: Сгенерировать решение студента на основе авторского и без комментов
                
                
                # TODO: Переделать вывод, добавить вердикт сразу
                print(f"[{folder}]",end=' ', flush=True)

                # out = get_testlib_answer(
                #     'solution.py',
                #     'author.py',
                #     'test.py'
                #     folder,
                #     TIMEOUT_PLATFORM,
                # )
                
                # TODO: Удалить сгенерированное решение студента из папки
                # TODO: Обнулить файловую систему в докере
                
        delete_testlib()


if __name__ == "__main__":
    # unittest.main()
    print(os.lstat('work_with_testlib.py'))