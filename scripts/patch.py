#!/usr/bin/env python3
import os
import hashlib
import shutil


def md5sum(filename: str) -> str:
    md5_hash = hashlib.md5()
    # Open the file in binary mode
    with open(filename, "rb") as file:
        for byte_block in iter(lambda: file.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()


def run_patch():
    c_dir = os.path.dirname(os.path.abspath(__file__))
    erl_top = os.getenv('ERL_TOP')
    if erl_top is None:
        raise Exception('ERL_TOP is not set')
    os.makedirs(f'{erl_top}/scripts', exist_ok=True)
    valgrind_md5 = md5sum(os.path.join(c_dir, 'valgrind_beamasm_update.escript'))
    target_valgrind_script = os.path.join(f'{erl_top}/scripts', 'valgrind_beamasm_update.escript')
    if os.path.exists(target_valgrind_script) == False or md5sum(target_valgrind_script) != valgrind_md5:
        print('patch valgrind_beamasm_update.escript ->', target_valgrind_script)
        shutil.copy2(os.path.join(c_dir, 'valgrind_beamasm_update.escript'), target_valgrind_script)
        os.chmod(target_valgrind_script, os.stat(target_valgrind_script).st_mode | os.X_OK)
    elixir_md5 = md5sum(os.path.join(c_dir, 'elixir'))
    target_elixir_script = shutil.which('elixir')
    if md5sum(target_elixir_script) != elixir_md5:
        print('patch elixir ->', target_elixir_script)
        shutil.copy2(os.path.join(c_dir, 'elixir'), target_elixir_script)
        os.chmod(target_elixir_script, os.stat(target_elixir_script).st_mode | os.X_OK)


if __name__ == '__main__':
    run_patch()
