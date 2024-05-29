import pandas as pd
import sys

# read JSON file
jsonfile = sys.argv[1]
df_j = pd.read_json(jsonfile)

# detect and extract columns of interest for portal
try:
    df_j['classification']
    columns = ['taxon_id','name','classification','taxon_rank','source']
    headers = ['Taxonomy ID','Scientific Name','Type','Rank','Source']
except KeyError:
    columns = ['taxon_id','name','taxon_rank','source']
    headers = ['Taxonomy ID','Scientific Name','Rank','Source']

df_c = df_j.to_csv(columns=columns, header=headers, index=False)
print(df_c.strip())