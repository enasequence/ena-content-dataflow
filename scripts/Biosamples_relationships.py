# !/usr/bin/env python3

import requests
import sys
import pandas as pd
import json
from datetime import datetime, date
###TODO: Dipayan mentioned adding the "webinSubmissionAccountId" in the sample json when updating it to retain original owner of sample - to be fixed by mid-Aug

#TODO: create flags for dev + prod options
# root_user = input("Enter the root username: ")  #will the username always be "Webin-40894" regardless of which db updates the sample?
# print(root_user)
# root_pwd = input("Enter the root password: ")
# print(root_pwd)

# obtaining Webin auth token
auth_url = 'https://wwwdev.ebi.ac.uk/ena/submit/webin/auth/token'
header = {"Content-Type": "application/json"} #can we add ;charset=UTF-8 here?
data = json.dumps(dict([("authRealms", [ "ENA" ]), ("password", "@!-dercane-!"), ("username", "Webin-40894")]))
print(data)
response = requests.post(auth_url, headers=header, data=data)

token = response.content.decode('utf-8') #to decode from bytes (indicated by b') to string
print("Token is" + token)

# input list of source + target accessions:
df = pd.read_csv('test_sample_accs_list.txt', sep='\t')
print("Input accessions list:\n")
print(df)
#print(df['ena_biosample_id'])
source_accs = df['ena_biosample_id'].tolist()
target_accs = df['ega_biosample_id'].tolist()
# print("ENA Biosample accessions are: " + str(source_accs))
# print("EGA Biosample accessions are: " + str(target_accs))
print()
for row in range(len(df)):
    print("linking ENA Biosample " + df.iloc[row,0] + " with EGA Biosample " + df.iloc[row,1])

# download SOURCE/ENA sample metadata:
biosamples_start = 'https://wwwdev.ebi.ac.uk/biosamples/samples/' #no auth token needed
for ena_bs in source_accs:
    biosamples_url = "{0}{1}".format(biosamples_start, ena_bs)
    r = requests.get(biosamples_url)
    #print(r.text)
    with open(f"{ena_bs}.json", "w") as f:
        f.write(r.text) #saves sample json object to file

# edit SOURCE/ENA sample metadata to include relationships block
    with open(f"{ena_bs}.json") as f:
        file_data = json.load(f) #load existing json data and returns it as a dictionary
        #print(file_data)
    file_data.pop("_links") #removes links array (will be added automatically after updating the biosample)
     #limitation this works only if there is a 1:1 mapping between ENA:EGA bs
    ega_bs = target_accs[source_accs.index(ena_bs)]
    array = {"relationships": [{"source": ena_bs, "type": "derived from", "target": ega_bs}]}  #.tolist() preserves order, so mapping between ENA->EGA bs acc should also be preserved
    print('Source sample: ' + ena_bs + ' derived from target sample: ' + ega_bs)
    file_data.update(array)

    with open(f"linked_{ena_bs}.json", 'w') as f:
        json.dump(file_data, f, indent=1) #converts python dictionary back into json string
       # NOTE: The above overwrites original json file - so if it originally had a relationships array this will be totally changed now
       # otherwise, relationship block is appended to end of json file

# submit updated Biosample json file:
    webin_auth = "?authProvider=WEBIN"
    update_url = "{0}{1}".format(biosamples_url, webin_auth)
    headers = {"Content-Type": "application/json;charset=UTF-8", "Accept": "application/hal+json", "Authorization": "Bearer {0}".format(token)}
    r = requests.put(update_url, headers=headers, data=open(f"linked_{ena_bs}.json", 'rb'))

# error messages:
    if r.status_code == 200:
        print('Biosamples successfully linked. Source sample: ' + ena_bs + ' derived from target sample: ' + ega_bs)
        print(r.text)
    else:
        print('Biosamples linking failed. See error file')
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%y_%H_%M_%S")
        error_file = open(f"error_{dt_string}.txt", "wb")
        error_file.write(r.content)
        error_file.close()

#TODO: clean up error message + file reporting
#TODO: add clear error messages for the requests