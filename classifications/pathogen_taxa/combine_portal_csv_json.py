import json
import sys

csvfile = sys.argv[1]
jsonfile = sys.argv[2]

portal_data = {}
with open(csvfile, 'r') as cfh:
    header = cfh.readline()
    for line in cfh:
        [tax_id, name, rank, source] = line.strip().split(',')
        portal_data[tax_id] = {'rank': rank, 'source': source}


with open(jsonfile, 'r') as jfh:
    j = json.load(jfh)

for x in j:
    try:
        add_data = portal_data[str(x['taxon_id'])]
        x['rank'] = add_data['rank']
        x['source'] = add_data['source']
    except KeyError:
        print("AAAAAAAAAAHHHHH")
        print(x)
        print("\n\n")

print(json.dumps(j, indent=4))