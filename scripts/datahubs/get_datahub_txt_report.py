
import argparse, os, sys, pickle
import datetime as dt
import pandas as pd

class Datahub:
    def __init__(self, dhub_name, title, pub_desc, status, coord_contact=[], projects=[], provider_webin=[],
                 latest_data='', run_count=0, latest_run='', analysis_count=0, latest_analysis='',
                 recc_status='', notes=''):
        self.dhub_name = dhub_name
        self.title = title
        self.pub_desc = pub_desc
        self.status = status
        self.coord_contact = coord_contact
        self.projects = projects
        self.provider_webin = provider_webin
        self.latest_data = latest_data
        self.run_count = run_count
        self.latest_run = latest_run
        self.analysis_count = analysis_count
        self.latest_analysis = latest_analysis
        self.recc_status = recc_status
        self.notes = notes

    def __repr__(self):
        description = f"""Object of class Datahub.{self.dhub_name}, {self.notes}"""
        return description


def main(opts):
    dhubtoprint = opts.datahub
    input_dir = opts.inputdir
    outputfolder = opts.outputdir
    today =
    dhub_path = input_dir + '/dhub_list.pkl'
    with open(dhub_path, 'rb') as f:
        dhub_list = pickle.load(f)

    ## get datahub metadata values
    text_out = ''
    for dhub in dhub_list:
        if dhub.dhub_name == dhubtoprint:
            text_out += f'''
            DATAHUB: {dhub.dhub_name}
            DESCRIPTION: {dhub.title}
            PUBLIC DESCRIPTION: {dhub.pub_desc}

            current status: {dhub.status}
            reccommended status: {dhub.recc_status}

            notes: {dhub.notes}

            Datahub webin account: {dhub.coord_contact}
            provider accounts: {dhub.provider_webin}

            Run count: {dhub.run_count} 
            Latest run: {dhub.latest_run}
            Analysis count: {dhub.analysis_count} 
            Latest analysis: {dhub.latest_analysis}

                        '''

    project_df = pd.read_csv(f"{input_dir}/projects.csv")
    project_mini = project_df.loc[project_df['datahub'] == dhubtoprint]
    tsv_string = project_mini.to_csv(sep='\t', index=False, lineterminator='\n')

    txt_report = text_out + tsv_string

    now = datetime.now()
    save_path = outputfolder + "/{dhubtoprint}-summaryreport" + now.strftime('%Y%m%d-%H%M') + '.txt'
    f = open(save_path, 'w')
    f.write(txt_report)
    f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Hub txt report argparser")

    parser.add_argument('-d', '--datahub', help="Name of Data Hub", default="none")
    parser.add_argument('i', '--inputdir', help="location of input dir")
    parser.add_argument('o', '--outputdir', help="location of output dir")

    opts = parser.parse_args()

    print(f'''
    --------------------------------------
     getting txt report for {opts.datahub}
    --------------------------------------
        ''')

    main(opts)