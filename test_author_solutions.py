import unittest
import os
import json
import subprocess

TIMEOUT_PLATFORM = 20

def get_out(cmd, folder):
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, timeout=TIMEOUT_PLATFORM)
        output = result.stdout.decode("utf-8")
    except subprocess.TimeoutExpired:
        output = f'TimeoutExpired in task {folder}'
        
    try:
        out = json.loads(output)
    except Exception:
        assert False, output

    return json.loads(output)

class RegressionTest(unittest.TestCase):
    def test_run(self):
        for folder in os.listdir(path='.'):
            if folder[:2] != 'de':
                continue
            with self.subTest(folder=folder):
                # TODO: Сгенерировать решение студента на основе авторского и без комментов
                
                
                # TODO: Переделать вывод, добавить вердикт сразу
                print(f"[{folder}]",end=' ', flush=True)

                # out = get_out(
                #     [
                #         "python",
                #         main_py,
                #         "-u",
                #         f"{fname}{user_py}",
                #         "-a",
                #         f"{fname}{author_py}",
                #         "-t",
                #         f"{fname}test.py",
                #         "--keep"
                #     ]
                # )
                
                # TODO: Удалить сгенерированное решение студента из папки

if __name__ == "__main__":
    # TODO: Скачать тестлибу
    
    unittest.main()
    
    # TODO: Удалить тестлибу
