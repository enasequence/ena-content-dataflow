import dash
from dash import dash_table
from dash import html
from dash import dcc
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash.dependencies import Input, Output, State
import datetime
from dash.exceptions import PreventUpdate

def uploading_dataframes():
    read_df = pd.read_csv('API_databases_files/API.reads.output.recent.csv', sep=",").dropna()
    seq_df = pd.read_csv('API_databases_files/API.sequence.output.recent.csv', sep=",").dropna()
    SQL_reads = pd.read_csv('API_databases_files/SQL-API.reads.log.csv', sep=",")
    return read_df, seq_df , SQL_reads


def grouping(df, column_names, resulted_column):
    grouped = df.groupby(column_names).size().reset_index(name=resulted_column)
    return grouped


def status_grouping (df, main_column_names, main_resulted_column, status_column_name, status_id, final_column_names, final_resulted_column):
    status_grouped_main = grouping(df, main_column_names,main_resulted_column)
    status_grouped = status_grouped_main.groupby(status_column_name).get_group(status_id)
    status_grouped_final = grouping(status_grouped, final_column_names,final_resulted_column)

    return status_grouped_final


def total_status_grouping(df, object_type):

    if object_type == 'project':
        main_column_name = ['Webin Account','Project ID','Project Status ID','Country']
        main_resulted_column = 'project in each webin'
        status_column_name = 'Project Status ID'
        final_column_names = ['Webin Account','Country']
        object = 'Project'

    elif object_type == 'sample':
        main_column_name = ['Webin Account','Project ID','Sample ID','Sample Status ID','Country']
        main_resulted_column = 'sample in each webin'
        status_column_name = 'Sample Status ID'
        final_column_names = ['Webin Account','Project ID','Country']
        object = 'Sample'

    elif object_type == 'experiment':
        main_column_name = ['Webin Account','Project ID','Sample ID','Sample Status ID','experiment_accession','Experiment Status ID','Country']
        main_resulted_column = 'experiment in each webin'
        status_column_name = 'Experiment Status ID'
        final_column_names = ['Webin Account','Project ID','Country']
        object = 'Experiments'


    status_grouped_private = status_grouping(df,main_column_name,main_resulted_column,status_column_name, 2 ,final_column_names,f'Private {object}')

    status_grouped_public = status_grouping(df,main_column_name,main_resulted_column,status_column_name, 4 ,final_column_names,f'Public {object}')

    status_grouped_suppressed = status_grouping(df,main_column_name,main_resulted_column,status_column_name, 5 ,final_column_names,f'Suppressed {object}')

    status_grouped_canceled = status_grouping(df,main_column_name,main_resulted_column,status_column_name, 3 ,final_column_names,f'Cancelled {object}')

    return status_grouped_private, status_grouped_public, status_grouped_suppressed, status_grouped_canceled


def status_table_merge(df1,df2,df3,df4,column_names):
    status_merge1=pd.merge(df1, df2, on=column_names, how='outer')
    status_merge2=pd.merge(status_merge1, df3, on=column_names, how='outer')
    status_merge = pd.merge(status_merge2, df4, on=column_names, how='outer')

    return status_merge

def count_table_merge (df1,df2,df3,df4, column_names):
    count_merge1 = pd.merge(df1, df2,
                                    on=column_names, how='outer')
    count_merge2 = pd.merge(count_merge1, df3,
                                    on=column_names, how='outer')
    count_merge_final = pd.merge(count_merge2, df4,
                                         on=column_names, how='outer')

    return count_merge_final


def webin_notes():
    # webin_notes.csv is a file contains the list of webin ids and their corresponding countries including the Notes and Tickets fields,
    # which provide as a template to save the Notes and related tickets (provided externally)
    webin_notes = pd.read_csv('API_databases_files/webin_notes.csv')
    return webin_notes

def project_notes():
    # project_notes.csv is a file contains the list of webin ids, projects and their corresponding countries including the Notes and Tickets fields,
    # which provide as a template to save the Notes and related tickets (provided externally)
    project_notes = pd.read_csv('API_databases_files/project_notes.csv')

    return project_notes

def formatting_notes_tickets(df,column_name):
    for i, value in df[column_name].iteritems():
        if value == None:
            df.at[i, column_name] = ''
    return df

def formatting_x_y(df,column_x, column_y ):
    df_mod = df.where(pd.notnull(df), None)
    for x, value_x in df_mod[column_x].iteritems():
        for y, value_y in df_mod[column_y].iteritems():
            if value_x != [None, np.nan, 'NaN'] and value_y != None:
                if x == y:
                    df_mod.at[x, column_x] = np.nan

            elif value_x != None and value_y == '':
                if x == y:
                    df_mod.at[x, column_x] = np.nan
    return df_mod



###################################
#                                 #
#             MAIN                #
####################################

#Uploading Dataframes
upload_df = uploading_dataframes()
read_df = upload_df[0]
seq_df = upload_df[1]
SQL_reads = upload_df[2]

# For runs
webin_run_grouped = grouping(SQL_reads, ['Webin Account','Country'] ,'Runs') #Total run Counts in each Webin

#For project
project_run_grouped = grouping(SQL_reads, ['Webin Account','Project ID','Country'] ,'Runs') #'Total run Counts in each Project'
webin_project_grouped = grouping(project_run_grouped, ['Webin Account','Country'] ,'Project') #'Total Project Counts in each Webin'

# For Samples
sample_run_grouped = grouping(SQL_reads, ['Webin Account','Project ID','Project Status ID','Sample ID','Country'] ,'Runs') #RUN ID Counts in each Sample
webin_sample_grouped = grouping(sample_run_grouped, ['Webin Account','Country'] ,'Samples') #'Sample Counts in each Webin
project_sample_grouped = grouping(sample_run_grouped, ['Webin Account','Project ID','Project Status ID','Country'] ,'Samples') #Sample Counts in each Project

#For Experiments
experiment_grouped= grouping(SQL_reads, ['Webin Account','Project ID','Sample ID','RUN ID','Country'] ,'Experiment ID Counts')
webin_experiment_grouped = grouping(experiment_grouped, ['Webin Account','Country'] ,'Experiments') #'Experiment ID Counts in each Webin'
project_experiment_grouped = grouping(experiment_grouped,['Webin Account','Project ID','Country'] ,'Experiments') #experiment Counts in each Project

# project status for each webin
project_status_grouped = total_status_grouping(SQL_reads, 'project')
sample_status_grouped = total_status_grouping(SQL_reads, 'sample')
ex_status_grouped = total_status_grouping(SQL_reads, 'experiment')

###### Table 2 merger ####
#sample merge
sample_status_merge = status_table_merge(sample_status_grouped[0],sample_status_grouped[1],sample_status_grouped[2],sample_status_grouped[3], ['Webin Account','Project ID', 'Country'])
#experiment merge
ex_status_merge = status_table_merge(ex_status_grouped[0], ex_status_grouped[1], ex_status_grouped[2], ex_status_grouped[3], ['Webin Account','Project ID', 'Country'])
#table merge
project_count_merge_final = count_table_merge(project_sample_grouped,sample_status_merge,project_experiment_grouped, ex_status_merge,['Webin Account','Project ID', 'Country']).fillna(0).replace('', 0)

##### Table 1 merger #####
#status merge
project_status_merge= status_table_merge(project_status_grouped[0], project_status_grouped[1], project_status_grouped[2], project_status_grouped[3], ['Webin Account', 'Country'])
#table merge
webin_count_merge = count_table_merge(webin_project_grouped, project_status_merge, webin_sample_grouped, webin_run_grouped,['Webin Account', 'Country'])
webin_count_merge_final =pd.merge(webin_count_merge, webin_experiment_grouped, on=['Webin Account', 'Country'], how='outer').fillna(0).replace('', 0)


#Upload the notes and tickets table
webin_notes = webin_notes()
project_notes = project_notes()

# Merge the stat tables with notes table
webin_count_merge = pd.merge(webin_count_merge_final,webin_notes, on=['Webin Account', 'Country'], how='outer')
project_count_merge =pd.merge(project_count_merge_final,project_notes, on=['Webin Account','Project ID', 'Country'], how='outer')

#Join Tables ( sequences with Reads)
country_inner_join = pd.merge(seq_df, read_df, on='country', how='outer')
country_inner_join1 = country_inner_join.where(pd.notnull(country_inner_join), 0)
country_inner_join_mod=country_inner_join1.rename(columns={'Submission Date_x': 'Sequences Submission Date','Submissions_x': 'Sequences Submissions','Submission Date_y': 'Reads Submission Date','Submissions_y': 'Reads Submissions'})



#######################
#                      #
#     DASHBOARD        #
########################


app = dash.Dash()

app.layout = html.Div([


    html.H1('Country Submission Dashboard'),
    html.P(['This Dashboard shows Country submission.',
            html.Br()]),

    dcc.Dropdown(id='country-dropdown', options=[{'label': i, 'value': i} for i in country_inner_join_mod['country'].unique()], value='United Kingdom'),
    dcc.Tabs([dcc.Tab(label='Country Graphs', children=[
    dcc.Graph(id='country-graph'),
    dcc.Graph(id='country-reads_graph'),
    dcc.Interval(
            id='interval-component',
            disabled=False,
            interval=24*2*30*60000, # in milliseconds
            n_intervals=0
    ),
    ]),
    dcc.Tab(label='Country Stats', children=[dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i, "editable": (True if i == 'Notes' or i == 'Related tickets' else False)} for i in webin_count_merge.columns],
    data=webin_count_merge.to_dict('records'),
    style_cell={'textAlign': 'center','padding': '10px','font_size': '10px'},
    style_header={
        'backgroundColor': 'white',
    },
    style_header_conditional=[
        {
            'if': {'column_id': ['Webin Account', 'Country', 'Project','Samples','Runs','Experiments']},
            'color': 'black',
            'fontWeight': 'bold',
            'border': '1px solid black'
        }
    ],
    style_cell_conditional=[
        {
            'if': {'column_id': ['Webin Account', 'Country', 'Project','Samples','Runs','Experiments']},
            'border': '1px solid black',
            'font_size': '14px'
        }
    ],
    ),
    html.Button(id="webin-save-button", children="Save Notes", n_clicks=0),
    html.Div(id="output-1", children="Press button to save changes to Notes"),
    dcc.Dropdown(id='webin-dropdown', options=[{'label': i, 'value': i} for i in project_count_merge['Webin Account'].unique()], value='United Kingdom'),
    dash_table.DataTable(
    id='webin-table',
    columns=[{"name": i, "id": i, "editable": (True if i == 'Notes' or i == 'Related tickets' else False)} for i in project_count_merge.drop(['Webin Account','Country'], axis=1).columns],
    data=project_count_merge.to_dict('records'),
    style_cell={'textAlign': 'center', 'font_size': '10px','padding': '1px'},
    style_header={
        'backgroundColor': 'white'
    },
    style_header_conditional=[
        {
            'if': {'column_id': ['Project ID', 'Samples', 'Experiments']},
            'backgroundColor': 'rgb(169,169,169)',
            'color': 'black',
            'fontWeight': 'bold',
            'border': '1px solid black'
        }
    ],
    style_cell_conditional=[
        {
            'if': {'column_id': ['Project ID', 'Samples', 'Experiments']},
            'backgroundColor': 'rgb(210, 210, 210)','border': '1px solid black', 'font_size': '12px'
        }
    ],

    ),
    html.Button(id="project-save-button", children="Save Notes", n_clicks=0),
    html.Div(id="output-2", children="Press button to save changes to Notes"),
    ])])])

@app.callback(
    Output('country-dropdown', 'options'),
    Output('country-dropdown', 'value'),
    Input('interval-component', 'n_intervals')

)
def update_metrics(n):
    seq_df =pd.read_csv('API_databases_files/API.sequence.output.recent.csv', sep=",").dropna()
    read_df = pd.read_csv('API_databases_files/API.reads.output.recent.csv', sep=",").dropna()
    country_inner_join = pd.merge(seq_df, read_df, on='country', how='outer')
    country_inner_join1 = country_inner_join.where(pd.notnull(country_inner_join), 0)
    country_inner_join_mod = country_inner_join1.rename(
        columns={'Submission Date_x': 'Sequences Submission Date', 'Submissions_x': 'Sequences Submissions',
                 'Submission Date_y': 'Reads Submission Date', 'Submissions_y': 'Reads Submissions'})
    country_options = [{'label': i, 'value': i} for i in country_inner_join_mod['country'].unique()]
    return country_options, country_options[0]['value']



@app.callback(
    Output('country-graph', 'figure'),
    Output('country-reads_graph', 'figure'),
    Input('country-dropdown', 'value'),

)

def update_graph(selected_country):
    now = datetime.datetime.now()
    now_str = now.strftime("%y-%m-%d")
    filtered_country=country_inner_join_mod[country_inner_join_mod['country']==selected_country]
    filtered_country_seq_mod = filtered_country.query ("`Sequences Submissions` > 0").drop_duplicates(subset="Sequences Submission Date")
    total_submissions =filtered_country_seq_mod['Sequences Submissions'].sum()
    if filtered_country_seq_mod['Sequences Submissions'].empty == True:
        line_fig = {
            "layout": {
                "xaxis": {
                    "visible": False
                },
                "yaxis": {
                    "visible": False
                },
                "annotations": [
                    {
                        "text": "No Sequence data found",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {
                            "size": 28
                        }
                    }
                ]
            }
        }
    else:
        line_fig=go.Figure(data=[go.Scatter(x=filtered_country_seq_mod['Sequences Submission Date'],y=filtered_country_seq_mod['Sequences Submissions'], name=f'Sequence Submissions in {selected_country}', mode='markers', marker=dict(
            size=filtered_country_seq_mod['Sequences Submissions'],
            sizemode='area',
            sizeref=2.*max(filtered_country_seq_mod['Sequences Submissions'])/(30.**2),
            sizemin=1
        ), hovertemplate="Country=%s<br>Submission Date=%%{x}<br>Submissions=%%{y}<extra></extra>"% selected_country)])
        line_fig.update_xaxes(
            ticklabelmode="period",
            rangeslider_visible = True,
            tickformatstops = [
            dict(dtickrange=[None, 1000], value="%H:%M:%S.%L"),
            dict(dtickrange=[1000, 60000], value="%H:%M:%S"),
            dict(dtickrange=[60000, 3600000], value="%H:%M"),
            dict(dtickrange=[3600000, 86400000], value="%H:%M"),
            dict(dtickrange=[86400000, 604800000], value="%e. %b"),
            dict(dtickrange=[604800000, "M1"], value="%e. %b"),
            ]
        )
        line_fig.update_xaxes(title_text="Submissions Date")
        line_fig.update_yaxes(title_text="Submissions")
        line_fig.update_layout(xaxis_range=['2020-01-01',now_str])
        line_fig.update_layout(
            title=f'Public Sequence Submissions in {selected_country} with total submissions of {int(total_submissions)} sequences',
            font=dict(
                family="Courier New, monospace",
                size=12,
                color="RebeccaPurple"
            )
        )

    filtered_country_reads = country_inner_join_mod[country_inner_join_mod['country'] == selected_country]
    filtered_country_reads_mod = filtered_country_reads.query("`Reads Submissions` > 0").drop_duplicates(subset="Reads Submission Date")

    total_submissions_reads = filtered_country_reads_mod['Reads Submissions'].sum()
    if filtered_country_reads_mod['Sequences Submissions'].empty == True:
        line_fig_reads = {
            "layout": {
                "xaxis": {
                    "visible": False
                },
                "yaxis": {
                    "visible": False
                },
                "annotations": [
                    {
                        "text": "No read data found",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {
                            "size": 28
                        }
                    }
                ]
            }
        }

    else:
        line_fig_reads = go.Figure(data=[go.Scatter(x=filtered_country_reads_mod['Reads Submission Date'],
                                              y=filtered_country_reads_mod['Reads Submissions'],
                                              name=f'Reads Submissions in {selected_country}', mode='markers',
                                              marker=dict(
                                                  size=filtered_country_reads_mod['Reads Submissions'],
                                                  sizemode='area',
                                                  sizeref=2. * max(filtered_country_reads_mod['Reads Submissions']) / (
                                                              30. ** 2),
                                                  sizemin=1
                                              ),
                                              hovertemplate="Country=%s<br>Submission Date=%%{x}<br>Submissions=%%{y}<extra></extra>" % selected_country)])
        line_fig_reads.update_xaxes(
            ticklabelmode="period",
            rangeslider_visible=True,
            tickformatstops=[
                dict(dtickrange=[None, 1000], value="%H:%M:%S.%L"),
                dict(dtickrange=[1000, 60000], value="%H:%M:%S"),
                dict(dtickrange=[60000, 3600000], value="%H:%M"),
                dict(dtickrange=[3600000, 86400000], value="%H:%M"),
                dict(dtickrange=[86400000, 604800000], value="%e. %b"),
                dict(dtickrange=[604800000, "M1"], value="%e. %b"),
            ]
        )
        line_fig_reads.update_xaxes(title_text="Submissions Date")
        line_fig_reads.update_yaxes(title_text="Submissions")
        line_fig_reads.update_layout(xaxis_range=['2020-01-01', now_str])
        line_fig_reads.update_layout(
            title=f'Public Read Submissions in {selected_country} with total submissions of {int(total_submissions_reads)} reads',
            font=dict(
                family="Courier New, monospace",
                size=12,
                color="RebeccaPurple"
            )
        )

    return line_fig,line_fig_reads

@app.callback(
    Output('table', 'data'),
    Output('webin-table', 'data'),
    Output('webin-dropdown', 'options'),
    Input('country-dropdown', 'value'),
    Input('webin-dropdown', 'value'),
    )

def update_stat_table (selected_country, webin_id):
    webin_notes = pd.read_csv('API_databases_files/webin_notes.csv')

    webin_count_merge = pd.merge(webin_count_merge_final, webin_notes, on=['Webin Account', 'Country'], how='outer')

    project_notes = pd.read_csv('API_databases_files/project_notes.csv')

    project_count_merge = pd.merge(project_count_merge_final, project_notes,
                                   on=['Webin Account', 'Project ID', 'Country'], how='outer')

    filtered_sql_reads = webin_count_merge[webin_count_merge['Country'] == selected_country]
    filtered_country_reads = project_count_merge[project_count_merge['Country'] == selected_country]
    filtered_webin_reads =  filtered_country_reads [filtered_country_reads['Webin Account'] == webin_id]
    data = filtered_sql_reads.to_dict('records')
    webin_data = filtered_webin_reads.to_dict('records')

    webin_options = [{'label': i, 'value': i} for i in filtered_country_reads['Webin Account'].unique()]

    return  data, webin_data , webin_options

@app.callback(
        Output("output-1","children"),
        [Input("webin-save-button","n_clicks")],
        [State("table","data")],
        )

def selected_webin_data_to_csv(nclicks,sub_table):
    if nclicks == 0:
        return "No Data Submitted"
        raise PreventUpdate #blocks callback from running


    else:
        webin_notes = pd.read_csv('API_databases_files/webin_notes.csv')

        webin_count_merge = pd.merge(webin_count_merge_final, webin_notes, on=['Webin Account', 'Country'], how='outer')

        sub_table_df = pd.DataFrame(sub_table)
        sub_table_df_mod_note = formatting_notes_tickets(sub_table_df,'Notes')
        sub_table_df_mod = formatting_notes_tickets(sub_table_df_mod_note,'Related tickets')

        webin_count_merge_note_raw = webin_count_merge[
            ['Webin Account', 'Country', 'Notes', 'Related tickets']].merge(
            sub_table_df_mod[['Webin Account', 'Country', 'Notes', 'Related tickets']], on=['Webin Account', 'Country'],
            how='left')

        webin_count_merge_note_formatted = formatting_x_y(webin_count_merge_note_raw,'Notes_x','Notes_y')
        webin_count_merge_all_formatted = formatting_x_y(webin_count_merge_note_formatted,'Related tickets_x','Related tickets_y')

        webin_count_merge_note = webin_count_merge_all_formatted.where(pd.notnull(webin_count_merge_all_formatted), '')

        webin_count_merge_note['Notes'] = webin_count_merge_note['Notes_x'].astype(str) + webin_count_merge_note['Notes_y'].astype(str)

        webin_count_merge_note['Related tickets'] = webin_count_merge_note['Related tickets_x'].astype(str) + \
                                                    webin_count_merge_note['Related tickets_y'].astype(str)

        pd.DataFrame(webin_count_merge_note[['Webin Account', 'Country', 'Notes', 'Related tickets']]).to_csv(
            'API_databases_files/webin_notes.csv', index=False)

        return "Data Submitted"



@app.callback(
    Output("output-2", "children"),
    [Input("project-save-button", "n_clicks")],
    [State("webin-table", "data")],
)
def selected_project_data_to_csv(project_nclicks, project_sub_table):
    if project_nclicks == 0:
        return "No Data Submitted"
        raise PreventUpdate #blocks callback from running

    else:
        project_notes = pd.read_csv('API_databases_files/project_notes.csv')

        project_count_merge = pd.merge(project_count_merge_final, project_notes, on=['Webin Account', 'Project ID', 'Country'], how='outer')

        project_sub_table_df = pd.DataFrame(project_sub_table)
        project_sub_table_df_mod_note = formatting_notes_tickets(project_sub_table_df, 'Notes')
        project_sub_table_df_mod = formatting_notes_tickets(project_sub_table_df_mod_note, 'Related tickets')

        project_count_merge_note_raw = project_count_merge[
            ['Webin Account','Project ID', 'Country', 'Notes', 'Related tickets']].merge(
            project_sub_table_df_mod[['Webin Account', 'Project ID', 'Country', 'Notes', 'Related tickets']], on=['Webin Account','Project ID', 'Country'],
            how='left')

        project_count_merge_note_formatted = formatting_x_y(project_count_merge_note_raw, 'Notes_x', 'Notes_y')
        project_count_merge_all_formatted = formatting_x_y(project_count_merge_note_formatted, 'Related tickets_x',
                                                         'Related tickets_y')

        project_count_merge_note = project_count_merge_all_formatted.where(pd.notnull(project_count_merge_all_formatted), '')

        project_count_merge_note['Notes'] = project_count_merge_note['Notes_x'].astype(str) + project_count_merge_note[
            'Notes_y'].astype(str)

        project_count_merge_note['Related tickets'] = project_count_merge_note['Related tickets_x'].astype(str) + \
                                                    project_count_merge_note['Related tickets_y'].astype(str)


        pd.DataFrame(project_count_merge_note[['Webin Account', 'Project ID', 'Country', 'Notes', 'Related tickets']]).to_csv(
            'API_databases_files/project_notes.csv', index=False)


        return "Data Submitted"


if __name__ == '__main__':
    app.run_server(debug=True)





