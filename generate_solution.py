import os
import re


def remove_comments(author_code):
    solution_code = []
    author_code = author_code.split('\n')
    for line in author_code:
        if bool(re.search('[а-яА-Я]', line)):
            continue
        process_line = line.lstrip()
        if process_line and process_line[0] == '#':
            i = line.find('#')
            line_after_comment = line[i+1:]
            spaces_after_comment = len(line_after_comment) - len(line_after_comment.lstrip())
            n = spaces_after_comment if spaces_after_comment <= 1 else 0
            line = line[:i] + line[i+n+1:]
        solution_code.append(line)
    return '\n'.join(solution_code)


def generate_solution_code(folder):    
    with open(f'{folder}/author.py', "r+", encoding='utf-8') as f:
        author_code = f.read()
    author_code = author_code.replace('AUTHOR', 'USERNAME')
    solution_code = remove_comments(author_code)
    with open(f'{folder}/solution.py','w') as f:
        f.write(solution_code)

