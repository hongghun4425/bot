import json
import os
import re
import subprocess


def escape_ansi(line):
    ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', line)


conda_json = escape_ansi(subprocess.check_output(['conda', 'info', '--json'], shell=True).decode('utf-8'))
conda_info = json.loads(conda_json)

simul_run_script = """\
@Echo simul_run Start
call "{conda_path}\\Scripts\\activate.bat" {venv}
call python "%~dp0\\..\\simul_run.py" 1 4 n
"""

ai_filter_script = """\
@Echo off
call "{conda_path}\\Scripts\\activate.bat" {venv}
call python "%~dp0\\..\\ai_filter.py" %1 %2
"""

base_script = """\
@Echo off
@Echo {bat_name} Start
set x=0
call "{conda_path}\\Scripts\\activate.bat" {venv}
@taskkill /f /im python.exe 2> NUL

:repeat
@tasklist | find "python.exe" /c > NUL
IF %ErrorLevel%==1 goto 1
IF NOT %ErrorLevel%==1 goto 0

:0
set /a x=%x%+1
echo x : %x%
::echo max : %max%
IF %x%==%max% @taskkill /f /im "python.exe"
goto repeat

:1
set x=0
set max=5000

start python "%~dp0\\..\\{file_name}.py"
timeout 5 > NUL
goto repeat
"""


def _kw_base_generator(venv):
    bats = dict.fromkeys(['collector', 'trader'])

    for bat_name in bats.keys():
        file_name = bat_name
        if bat_name == 'collector':
            file_name += '_v3'
        bats[bat_name] = base_script.format(bat_name=bat_name, file_name=file_name,
                                            conda_path=conda_info['conda_prefix'], venv=venv)
    return bats


def _create_bats(bats):
    for bat_name, script in bats.items():
        bat_path = f".\\bat\\{bat_name}.bat"
        if os.path.exists(bat_path):
            os.remove(bat_path)
        with open(bat_path, "w+") as bat_file:
            bat_file.write(script)


def generate_scripts(venv_32, venv_64):
    bats = _kw_base_generator(venv_32)
    bats['simul_run'] = simul_run_script.format(conda_path=conda_info['conda_prefix'], venv=venv_64)
    bats['ai_filter'] = ai_filter_script.format(conda_path=conda_info['conda_prefix'], venv=venv_64)

    _create_bats(bats)


def generate_scripts_32():
    bats = _kw_base_generator('base')
    _create_bats(bats)


if __name__ == '__main__':
    bit = None
    for ch in conda_info['channels']:
        if '64' in ch:
            bit = 64
            break
        elif '32' in ch:
            bit = 32
            break

    if bit == 32:
        generate_scripts_32()

    elif bit == 64:
        venv_32 = 'py37_32'
        venv_64 = 'py37_64'

        env_names = [os.path.split(env)[1] for env in conda_info['envs']]
        while venv_32 not in env_names or venv_64 not in env_names:
            if venv_32 not in env_names:
                print(f'{venv_32} 이름의 가상환경을 찾을 수 없습니다.')
                venv_32 = input('32비트 가상환경 이름을 입력해주세요: ')
            if venv_64 not in env_names:
                print(f'{venv_64} 이름의 가상환경을 찾을 수 없습니다.')
                venv_64 = input('64비트 가상환경 이름을 입력해주세요: ')

        generate_scripts(venv_32, venv_64)

    print('배치파일을 성공적으로 생성하였습니다.')
