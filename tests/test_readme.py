
def main():
    """
    The following will test the code blocks in the README file,
    It will run each line and make sure it doesn't raise any errors
    """
    with open('../README.md', 'r') as f:
        lines = f.readlines()

    code = []
    inside_code_block = False

    for line in lines:
        if '```py' in line:
            inside_code_block = True

        elif '```' in line:
            inside_code_block = False

        elif inside_code_block:
            if 'db_path' in line:
                replace_path = line.replace('data/', '../data/')
                code.append(replace_path)
                print(f'{line=}, \n{replace_path=}')
            else:
                code.append(line)

    code = ''.join(code)
    print(code)
    exec(code)


if __name__ == '__main__':
    main()
