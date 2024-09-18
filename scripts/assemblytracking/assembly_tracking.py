#!/usr/bin/python3

# Copyright [2024] EMBL-European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os, sys, argparse, subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="assembly tracking argparser")
    parser.add_argument('-p', '--project', help="Project to track DToL, ASG or ERGA", default="none")
    parser.add_argument('-c', '--config', help="config file path", default="config_private.yaml")
    parser.add_argument('-w', '--workingdir', help="location of tracking file folders",
                        default="scripts/assemblytracking/")
    parser.add_argument('-a', '--action', help="action to perform, Options: sql(get new accessions), add(add and track) or track(track only)",
                        default="scripts/assemblytracking/")
    opts = parser.parse_args()

    print(f'''
    +===============================================================+
    |              European Nucleotide Archive (ENA)                |
    |                     Assembly Tracking                         |
    |                    Project tracked: {opts.project}                      |
    +===============================================================+
    ''')


    # set file path strings (needs to be outside of main function)
    tracking_files_path = f'{opts.project}-tracking-files'
    # set the location of the downloaded excel file for reading
    exceldl_path = f'{tracking_files_path}/{opts.project} assembly tracking.xlsx'
    # FILE OUTPUTS SAVE LOCATIONS
    tracking_file_path = f'{tracking_files_path}/tracking_file.txt'

    if opts.action == 'sql':
        print(f'''
    +===============================================================+
    |      Action 'sql' selected. Running database queries...       |
    +===============================================================+
        ''')
        sqlargs = ['-p',f'{opts.project}', '-c',f'{opts.config}', '-w', f'{opts.workingdir}']
        command = [sys.executable, 'sql_processingatENA.py'] + sqlargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
    elif opts.action == 'add':
        print(f'''
    +===============================================================+
    |                    Action 'add' selected                      |
    |  Adding new assemblies to tracking file and checking status   |
    +===============================================================+
        ''')
        addtargs = ['-p', f'{opts.project}', '-w', f'{opts.workingdir}']
        addncbiargs = ['-p', f'{opts.project}', '-c', f'{opts.config}', '-w', f'{opts.workingdir}']

        command = [sys.executable, 'step1_add_assemblies_to_file.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step2_releasingsequences_ENA.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step3_ENA_linking.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step4_processingatNCBI.py'] + addncbiargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'export_summarytracking_files.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
    elif opts.action == 'track':
        print(f'''
    +===============================================================+
    |                  Action 'track' selected                      |
    |             Checking status of tracked assemblies             |
    +===============================================================+
        ''')
        addtargs = ['-p', f'{opts.project}', '-w', f'{opts.workingdir}']
        addncbiargs = ['-p', f'{opts.project}', '-c', f'{opts.config}', '-w', f'{opts.workingdir}']

        command = [sys.executable, 'step2_releasingsequences_ENA.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step3_ENA_linking.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step4_processingatNCBI.py'] + addncbiargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'export_summarytracking_files.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
    elif opts.action == 'all':
        print(f'''
    +===============================================================+
    |                   Action 'all' selected!                      |
    |                Running all tracking scripts                   |
    +===============================================================+
        ''')
        sqlargs = ['-p',f'{opts.project}', '-c',f'{opts.config}', '-w', f'{opts.workingdir}']
        command = [sys.executable, 'sql_processingatENA.py'] + sqlargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)

        addtargs = ['-p', f'{opts.project}', '-w', f'{opts.workingdir}']
        addncbiargs = ['-p', f'{opts.project}', '-c', f'{opts.config}', '-w', f'{opts.workingdir}']

        command = [sys.executable, 'step1_add_assemblies_to_file.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step2_releasingsequences_ENA.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step3_ENA_linking.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'step4_processingatNCBI.py'] + addncbiargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        command = [sys.executable, 'export_summarytracking_files.py'] + addtargs
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
    else:
        print('no action selected!')
