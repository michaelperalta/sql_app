import streamlit as st
import pandas
from pandasql import sqldf
import re
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from re import sub
from streamlit_ace import st_ace
from pypika import Query, Table, Field, functions as fn
from sql_formatter.core import format_sql
import random_facts
from generate_sql_query import gen_query
from create_container import dimension_and_metric_container, relationship_container, sql_container, results_container

st.set_page_config(layout='wide')

## Session State Variables
if 'query_text' not in st.session_state:
    st.session_state.query_text = ''
    
if 'random_fact' not in st.session_state:
    st.session_state.random_fact = random_facts.get_fact()

if 'data_sources' not in st.session_state:
    st.session_state.data_sources = ''
    
if 'input_type' not in st.session_state:
    st.session_state.input_type = ''
    
if 'relationship_count' not in st.session_state:
    st.session_state.relationship_count = 0

if 'relationships_selected' not in st.session_state:
    st.session_state.relationships_selected = []
    
if 'relationship_remove_list' not in st.session_state:
    st.session_state.relationship_remove_list = []
    
if 'count' not in st.session_state:
    st.session_state.count = 0

if 'metrics_selected' not in st.session_state:
    st.session_state.metrics_selected = []
    
if 'remove_list' not in st.session_state:
    st.session_state.remove_list = []
    
if 'table_to_query' not in st.session_state:
    st.session_state.table_to_query = None
    
if 'filter_count' not in st.session_state:
    st.session_state.filter_count = 0

if 'filters_selected' not in st.session_state:
    st.session_state.filters_selected = []
    
if 'filter_remove_list' not in st.session_state:
    st.session_state.filter_remove_list = []

## Functions

def get_aggregation_function(selected_agg,selected_agg_column):
    if selected_agg == 'Avg':
        return fn.Avg(selected_agg_column)
    if selected_agg == 'Count':
        return fn.Count(selected_agg_column)
    if selected_agg == 'Count Distinct':
        return fn.Count('distinct ' + selected_agg_column)
    if selected_agg == 'Max':
        return fn.Max(selected_agg_column)
    if selected_agg == 'Min':
        return fn.Min(selected_agg_column)
    if selected_agg == 'Sum':
        return fn.Sum(selected_agg_column)

def snake_case(s):
  return '_'.join(
    sub('([A-Z][a-z]+)', r' \1',
    sub('([A-Z]+)', r' \1',
    s.replace('-', ' ').replace('(','').replace(')',''))).split()).lower()

def get_columns(data_sources,table_to_query,column_type):
    if column_type == 'relationship':
        # if table_to_query == None:   
        output = [[],[]]                 
        for i,data_source in enumerate(data_sources):
            for column in globals()[data_source].columns.sort_values():
                full_column = data_source + '.' + column
                output[i].append(full_column)
            output[i].sort()
            output[i].insert(0,'Select Column')
        # else:
        #     output = []
        #     for data_source in data_sources:
        #         if data_source == table_to_query:
        #             for column in globals()[data_source].columns.sort_values():
        #                 full_column = data_source + '.' + column
        #                 output.append(full_column)
        #     output.sort()
    if column_type == 'dimension':
        output = []
        if table_to_query == None:                    
            for data_source in data_sources:
                for column in globals()[data_source].columns.sort_values():
                    full_column = data_source + '.' + column
                    output.append(full_column)
            output.sort()
            output.insert(0,'Select Column')
        else:
            for data_source in data_sources:
                if data_source == table_to_query:
                    for column in globals()[data_source].columns.sort_values():
                        full_column = data_source + '.' + column
                        output.append(full_column)
            output.sort()
    if column_type == 'metric':
        output = []
        if table_to_query == None:                    
            for data_source in data_sources:
                for column in globals()[data_source].columns.sort_values():
                    full_column = data_source + '.' + column
                    output.append(full_column)
            output.sort()
            output.insert(0,'Select Column')
        else:
            for data_source in data_sources:
                if data_source == table_to_query:
                    for column in globals()[data_source].columns.sort_values():
                        full_column = data_source + '.' + column
                        output.append(full_column)
            output.sort()
            output.insert(0,'Select Column')
    
    return output

def get_selections(select_list,remove_list,key_a,key_b,default_state_a,default_state_b):
    selections = [selection for selection in st.session_state[select_list] if selection not in st.session_state[remove_list]]
        
    selections = [[st.session_state['%s_%s' % (key_a,selection)],st.session_state['%s_%s' % (key_b,selection)]] for selection in selections]
    
    selections = [selection for selection in selections if selection[0] != default_state_a and selection [1] != default_state_b]
            
    return selections

def get_filter_selections(filter_select_list,filter_remove_list,input_keys,default_states):
    
    selections = []
    
    for s in st.session_state[filter_select_list]:
        if s not in st.session_state[filter_remove_list]:
            
            filter_column = st.session_state['filter_column_%s' % s]
            filter_operator = st.session_state['filter_%s' % s]
            
            if filter_column not in default_states and filter_operator not in default_states:
                filter_value = st.session_state['filter_value_%s' % s]
                if filter_value not in default_states and filter_value != '' and filter_value != []:
                    selections.append([filter_column,filter_operator,filter_value])
                    
    return selections
                               
pysqldf = lambda q: sqldf(q, globals())

## UI
st.sidebar.subheader('Data Sources')
prototype_help_text = 'When selected, only the first 1,000 rows of data will be uploaded from each data source.'
prototype_mode = st.sidebar.checkbox('Prototype Mode',value=True,help=prototype_help_text)
data = st.sidebar.file_uploader('Data Sources',type=['csv','tsv'],label_visibility='collapsed',accept_multiple_files=True,help='Upload your data sources to begin analysis. Data sources are automatically transformed into tables.')

if data:
    data_sources = []
    for d in data:
        data_source = snake_case(d.name.split('.')[0])
        data_sources.append(data_source)
        file_type = d.name.split('.')[-1:][0]
        if file_type == 'tsv':
            if prototype_mode == True:
                globals()[data_source] = pandas.read_csv(d,sep='\t',nrows=1000)
            else:
                globals()[data_source] = pandas.read_csv(d,sep='\t')
            globals()[data_source].columns = [snake_case(column) for column in globals()[data_source]]
        if file_type == 'csv':
            if prototype_mode == True:
                globals()[data_source] = pandas.read_csv(d,nrows=1000)
            else:
                globals()[data_source] = pandas.read_csv(d)
            globals()[data_source].columns = [snake_case(column) for column in globals()[data_source]]
    st.session_state.data_sources = data_sources

    input_columns = st.columns(3)
    st.markdown('---')

if data:
    input_columns[0].write('Input Type')
    input_type = input_columns[0].radio('Select an Input Type',['SQL','Insight Builder'],index=0,horizontal=True,label_visibility='collapsed')
    st.session_state.input_type = input_type
        
####### SQL Interface

empty_state = st.empty()

with empty_state:
    st.markdown("<h1 style='text-align: center; color: gray; vertical-align: middle;'>%s</h1>" % st.session_state.random_fact, unsafe_allow_html=True)
    
if st.session_state.input_type == 'SQL' and data:
    empty_state.empty()

    text,metadata = st.columns([3,2])

    with text: 
        st.subheader('Query')

        text = st_ace(language='sql',placeholder='Enter SQL Query',height=300)
        st.session_state.query_text = text

    with metadata:
        st.subheader('Table Metadata')
        tabs = st.tabs(data_sources)

        for i in range(len(data_sources)):

            with tabs[i]: 
                
                table_metadata = pandas.DataFrame(
                    [[snake_case(column),globals()[data_sources[i]][column].iat[0],str(globals()[data_sources[i]][column].dtypes)] for column in globals()[data_sources[i]].columns],
                    columns=['column','sample_value','type']
                    ).sort_values(by='column')
                
                AgGrid(
                    table_metadata,
                    fit_columns_on_grid_load=True,
                    width='100%',
                    key='metadata_%s' % data_sources[i])
    
    if text:
        output_data = pysqldf(text)
        st.session_state.query_text = text    
        results_container(output_data)

######### UI Interface

if st.session_state.input_type == 'Insight Builder' and data:
    empty_state.empty()
    
    query_text = None
    
    if len(data_sources) > 1:
        input_columns[1].write('Merge Data Sources')
        merging_options_radio = input_columns[1].radio('Merge Data Sources',options=['Yes','No'],index=0,key='merging_options_radio',horizontal=True,label_visibility='collapsed')
        
        if merging_options_radio == 'Yes':
            data_dfs = {}
            
            for data_source in data_sources:
                data_dfs.update({data_source: globals()[data_source]})
            
            relationship_columns = get_columns(data_sources,st.session_state.table_to_query,'relationship')

            relationship_container(relationship_columns[0],relationship_columns[1])
                
            selected_relationships = get_selections('relationships_selected','relationship_remove_list','data_source_a','data_source_b','Select Column','Select Column')
                            
            select_columns = get_columns(data_sources,st.session_state.table_to_query,'dimension')
            metric_columns = get_columns(data_sources,st.session_state.table_to_query,'metric')
            
            selected_columns = dimension_and_metric_container(metric_columns, select_columns,data_dfs)
            
            selected_metrics = get_selections('metrics_selected','remove_list','agg','metric','Select Aggregation','Select Column')
            
            selected_filters = get_filter_selections('filters_selected','filter_remove_list',['filter_column','filter','filter_value'],['Select Column','Select Filter','Select Value'])

            if (selected_columns or selected_metrics) and selected_relationships:
                query_text = gen_query(data_sources,selected_relationships,selected_columns,selected_metrics,selected_filters)
                    
        if merging_options_radio == 'No':
            input_columns[2].write('Select Table to Query')
            st.session_state.table_to_query = input_columns[2].selectbox('Select Table to Query',index=0,options=data_sources,key='table_to_query_input',label_visibility='collapsed')
            
            data_source = st.session_state.table_to_query

            select_columns = get_columns(data_sources,st.session_state.table_to_query,'dimension')
            metric_columns = get_columns(data_sources,st.session_state.table_to_query,'metric')
            
            selected_columns = dimension_and_metric_container(metric_columns, select_columns,data_source)
            
            selected_metrics = get_selections('metrics_selected','remove_list','agg','metric','Select Aggregation','Select Column')
            
            selected_filters = get_filter_selections('filters_selected','filter_remove_list',['filter_column','filter','filter_value'],['Select Column','Select Filter','Select Value'])
            
            if selected_columns or selected_metrics:
                query_text = gen_query([data_source],[],selected_columns,selected_metrics,selected_filters)

    if len(data_sources) == 1:
        data_dfs = {}
        
        for data_source in data_sources:
            data_dfs.update({data_source: globals()[data_source]})

        query_text = None
        
        select_columns = get_columns(data_sources,st.session_state.table_to_query,'dimension')
        metric_columns = get_columns(data_sources,st.session_state.table_to_query,'metric')
        selected_columns = dimension_and_metric_container(metric_columns, select_columns,data_dfs)
        
        selected_metrics = get_selections('metrics_selected','remove_list','agg','metric','Select Aggregation','Select Column')
        
        selected_filters = get_filter_selections('filters_selected','filter_remove_list',['filter_column','filter','filter_value'],['Select Column','Select Filter','Select Value'])
        
        if selected_columns or selected_metrics:
            query_text = gen_query([data_source],[],selected_columns,selected_metrics,selected_filters)
    
    if query_text:
        output_data = pysqldf(query_text)
        
        ## SQL Container
        sql_container(query_text)
    
        ## Results Container
        results_container(output_data)