import sys, os, argparse
import subprocess

"""
Note: this script is a wrapper around: https://github.com/ebi-wp/EBISearch-webservice-clients/blob/master/python/ebeye_requests.py

Maximum number of results returned by ebeye_requests.py is 100. This script loops
over all results in steps of 100 until there are no more. Example:

python sync/ebeye_request_loop.py --domain sra-experiment-covid19-host --filter "TAXON:9606" --fields "id,PROJECT,TAXON"

"""


# parse inputs
parser = argparse.ArgumentParser(
    description="",
)
parser.add_argument('--domain', help="EBISearch domain")
parser.add_argument('--filter', help="EBISearch filter")
parser.add_argument('--fields', help="fields to return")
opts = parser.parse_args(sys.argv[1:])

if not ( opts.domain and opts.filter and opts.fields ):
    sys.stderr.write("Please provide domain, filter and fields")
    sys.exit(1)

fields = opts.fields.split(",")
print("\t".join(fields))

start = 0
size  = 100
while True:
    cmd = [
        'ebeye_requests.py', 'getResults', opts.domain,
        f"\'{opts.filter}\'", f"\'{opts.fields}\'", f"--size {size}", f"--start {start}"
    ]
    sys.stderr.write(" ".join(cmd) + "\n")

    result = subprocess.run(" ".join(cmd), capture_output=True, shell=True, encoding='utf-8')
    lines = result.stdout.split("\n")
    if len(lines) < len(fields):
        break

    step = len(fields)
    for i in range(0, len(lines), step):
        if i + step > len(lines):
            break
        print("\t".join(lines[i:i+step]))

    start += size
