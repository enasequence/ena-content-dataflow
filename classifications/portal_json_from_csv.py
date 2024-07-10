import pandas as pd
import argparse

def get_arg ():
    parser = argparse.ArgumentParser(description='Convert CSV to JSON.')
    parser.add_argument('-i','--input', type=str, help='Path to the input CSV file' , required=True )
    parser.add_argument('-o' , '--output', type=str, help='Path to the output JSON file' , required=True )

    args = parser.parse_args()

    return args
args = get_arg()

if __name__ == "__main__":
    try : 
        # Read the CSV file into a DataFrame
        df = pd.read_csv(args.input)
        new_headers = {
            'Taxonomy ID': 'taxon_id',
            'Scientific Name': 'name',
            'classification': 'classification',
            'Rank': 'taxon_rank',
            'Source': 'source',
            'Common Name': 'common_name'
        }

        # Rename the headers
        df.rename(columns=new_headers, inplace=True)
        
        # Convert the DataFrame to JSON format
        df.to_json(args.output, orient='records', indent=4)

        print(f"Successfully converted {args.input} to {args.output}")
    
    except Exception as e : 
        print (e)
