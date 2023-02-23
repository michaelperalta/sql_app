import streamlit as st
from remove_from_remove_list import remove
from increment_counter import increment
from sql_formatter.core import format_sql
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder

aggregate_options = [
    'Select Aggregation',
    'Avg',
    'Count',
    'Count Distinct',
    'Max',
    'Min',
    'Sum'
    ]

filter_options = [
    'Select Filter',
    'equals',
    'does not equal',
    'greater than', 
    'greater than or equal to',
    'less than', 
    'less than or equal to',
    'contains',
    'does not contain',
    'is',
    'is not'
    ]

def dimension_and_metric_container(metric_columns,select_columns,data_df):
    with st.expander('Dimensions, Metrics and Filters',expanded=True):
        selected_columns = dimension_container(select_columns)
        metric_container(metric_columns)
        filter_container(select_columns,data_df)
        
        # st.button('Add Metric',on_click=increment,kwargs={'session_state_attribute':'count','session_state_attribute_list':'metrics_selected'})
        
    return selected_columns

def dimension_container(select_columns):
    st.write("Select Dimension(s)")
    dimension_cols = st.columns([2,1])
            
    selected_columns = dimension_cols[0].multiselect('Select Column(s)',options=select_columns,key='dimensions',label_visibility='collapsed')
    
    return selected_columns

def metric_container(metric_columns):
    st.markdown('---')
    st.write('Select Metric(s)')
    cols = st.columns(3)

    
    for i in st.session_state.metrics_selected:
        if i not in st.session_state.remove_list:
            cols[0].selectbox('Select Aggregation',options=aggregate_options,key='agg_%s' % i,label_visibility="collapsed")
            cols[1].selectbox('Select Column',options=metric_columns,key='metric_%s' % i,label_visibility="collapsed")
            cols[2].button('Remove',key=i,on_click=remove,kwargs={'session_state_attribute':'remove_list','session_state_value':i})
    
    st.button('Add Metric',on_click=increment,kwargs={'session_state_attribute':'count','session_state_attribute_list':'metrics_selected'})
    
def filter_container(select_columns,data_df):
    st.markdown('---')
    st.write('Select Filter(s)')
    cols = st.columns([4,2,4,1])
    
    for i in st.session_state.filters_selected:
        if i not in st.session_state.filter_remove_list:
            column = cols[0].selectbox('Select Column',options=select_columns,key='filter_column_%s' % i,label_visibility="collapsed")
            operator = cols[1].selectbox('Select Filter',options=filter_options,key='filter_%s' % i,label_visibility="collapsed")
            
            if operator in ['equals','does not equal'] and column != 'Select Column':
                table = column.split('.')[0]
                selected_column = column.split('.')[-1]
                filter_values = data_df['%s' % table][selected_column].drop_duplicates().sort_values().values.tolist()
                filter_values.insert(0,'Select Value')
                cols[2].multiselect('Select Filter',options=filter_values,key='filter_value_%s' % i,label_visibility="collapsed")
            if operator in ['greater than','greater than or equal to','less than','less than or equal to'] and column != 'Select Column':
                cols[2].number_input('Input Value',key='filter_value_%s' % i,label_visibility="collapsed",step=1)
            if operator in ['contains','does not contain'] and column != 'Select Column':
                cols[2].text_input('Input Value',placeholder='Input Value',key='filter_value_%s' % i,label_visibility="collapsed")
            if operator in ['is','is not']:
                cols[2].selectbox('Input Value',options=['null','blank'],key='filter_value_%s' % i,label_visibility="collapsed")
                cols[3].button('Remove',key='remove_filter_%s' % i,on_click=remove,kwargs={'session_state_attribute':'filter_remove_list','session_state_value':i})
            if operator not in ['is','is not']:
                cols[3].button('Remove',key='remove_filter_%s' % i,on_click=remove,kwargs={'session_state_attribute':'filter_remove_list','session_state_value':i})
            
    
    st.button('Add Filter',on_click=increment,kwargs={'session_state_attribute':'filter_count','session_state_attribute_list':'filters_selected'})

    
def relationship_container(relationship_columns_a,relationship_columns_b):
    with st.expander('Relationships',expanded=True):
        cols = st.columns([3,1,3,1])
        for i in st.session_state.relationships_selected:
            if i not in st.session_state.relationship_remove_list:
                cols[0].selectbox('Select Column',options=relationship_columns_a,key='data_source_a_%s' % i,label_visibility="collapsed")
                cols[1].write('equals')
                cols[2].selectbox('Select Column',options=relationship_columns_b,key='data_source_b_%s' % i,label_visibility="collapsed")
                cols[3].button('Remove',key='remove_relationship_%s' % i,on_click=remove,kwargs={'session_state_attribute':'relationship_remove_list','session_state_value':i})

        st.button('Add Relationship',on_click=increment,kwargs={'session_state_attribute':'relationship_count','session_state_attribute_list':'relationships_selected'})
        
def sql_container(query_text):
    st.sidebar.markdown('---')

    st.sidebar.subheader('SQL')
    st.sidebar.code(format_sql(query_text),language='sql')

def results_container(output):
    st.markdown('---')
    with st.expander('Results',expanded=True):   
        st.caption('Export data by right clicking on table and going to export.')
        
        gb = GridOptionsBuilder.from_dataframe(output)
        gb.configure_pagination(paginationPageSize=10)
        gb.configure_side_bar()
        gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gridOptions = gb.build()
        AgGrid(
            output,
            gridOptions=gridOptions,
            fit_columns_on_grid_load=True,
            width='100%'
            )