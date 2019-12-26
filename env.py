import os as _os
import sys as _sys

__all__ = []

def __prompt(question, dtype = bool, default = False):
    question += ' ' if question.endswith(':') else ': '
    if dtype == bool:
        question += '(Y/n) ' if default else '(y/N) '
        
    elif default is not None:
        question += f'({default}) '
    resp = input(question)
    if resp == '':
        return default
    if dtype == bool:
        return default if resp.lower() not in ['y','n'] else resp.lower() == 'y'
    while True:
        try:
            return dtype(resp)
        except:
            print('Invalid input', file=_sys.stderr)
            resp = input(question)

if __name__ == '__main__':
    # establish the root directory for this local env
    root = _os.getcwd()
    # prompt user for env variables
    if not __prompt('Do you have the bbstat source application built locally on your machine?', default=True):
        source_url = 'https://bbstat.herokuapp.com'
    else:
        port = __prompt('Which port number it is configured to run on?', dtype=int, default=3000)
        source_url = f'http://localhost:{port}'
    compiled_path = __prompt('Specify a path for all compiled csv files', dtype=str, default='./build')
    # ensure compiled_path is absolute
    compiled_path = _os.path.abspath(_os.path.expanduser(compiled_path))
    # make directory for compiled_path if it does not exist
    if not _os.path.exists(compiled_path):
        _os.makedirs(compiled_path)
    # construct .env file
    ENV = [
        f'ROOT={root}',
        f'SOURCE_URL={source_url}',
        f'COMPILED_PATH={compiled_path}',
    ]
    print('','Local Env Config:', *(f'  {l}' for l in ENV), sep='\n')
    # write .env file
    with open('.env', 'w') as f:
        print(*ENV, sep='\n', file=f)
    exit()

def __parse_env():
    global __all__
    # parse .env file and add all key value pairs to globals dict
    with open('.env') as f:
        for k,v in [(l[:l.index('=')],l[l.index('=')+1:]) for l in f.read().strip().split('\n')]:
            __all__ = __all__ + [k]
            globals()[k] = v

# check to ensure a .env file exists locally
if _os.path.exists('.env'):
    __parse_env()
else:
    print(*[
        'Warning! local env is not setup. Open up terminal and run:',
        '  ❯ python env.py',
    ], sep='\n', file=_sys.stderr)