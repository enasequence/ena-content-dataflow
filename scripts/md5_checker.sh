#!/usr/bin/bash

# Copyright [2020] EMBL-European Bioinformatics Institute
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



# This script compares md5 values for the read files submitted using the covid-utils tool. Things to note:

#1) input read files must have some kind of hash at the end of the filename (examples provided in script comments)
#2) change the 'submitted_read_files' variable to point to the location where your test files are located
#3) this script works successfully on the ebi-cli cluster; but does not currently work on a Mac or PyCharm terminal due to discrepancies between Linux (GNU) and Mac (BSD) outputs of the 'md5sum' command


submitted_read_files=$(basename -a `ls /c/Users/zahra/Documents/test/{*fastq*,*cram*,*bam*}`) #change dir according to where read files are #remember all submitted read files have to have hash at the end of filename e.g: testread.fastq.gz.3396695902fd7ab76d5aaa2fe6297cdc  

for file in $submitted_read_files; do
	md5sum $file > ${file}.md5     #testread.fastq.gz.3396695902fd7ab76d5aaa2fe6297cdc.md5 
	new_hash=`cut -d ' ' -f 1 ${file}.md5`  #isolates the hash that we just calculated, inside of the .md5 file we just generated: 3396695902fd7ab76d5aaa2fe6297cdc 
	original_hash=`echo ${file##*.}` #remove everything before the last full stop in filename- so we are left with the hash only
	echo original hash of submitted file $file is $original_hash
	echo newly generated hash for submitted file is $new_hash
	if [[ $new_hash == $original_hash ]];
	then
		echo
		echo submitted and calculated md5 values match - file $file PASSED verification
		echo
		mv $file ${file%.*}  #to rename the 'testread.fastq.gz.3396695902fd7ab76d5aaa2fe6297cdc' files to just 'testread.fastq.gz' so they can be used for downstream analysis
	else
		echo
		echo submitted and calculated md5 values DO NOT match - file $file FAILED verification
		echo
	fi
	
done
