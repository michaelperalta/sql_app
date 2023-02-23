from sql_formatter.core import format_sql

# tables = ['highest_hollywood_grossing_movies','movies_on_streaming_platforms']

# selected_columns = [
#   "highest_hollywood_grossing_movies.cleaned_title",
#   "movies_on_streaming_platforms.title"
# ]

# join_columns = [
#     [
#   "highest_hollywood_grossing_movies.cleaned_title",
#   "movies_on_streaming_platforms.title"
# ],
#     [
#   "highest_hollywood_grossing_movies.id",
#   "movies_on_streaming_platforms.id"
# ]
# ]

# selected_aggregates = [
#     [
#       'Avg','highest_hollywood_grossing_movies.rating'
#       ]
#     ]



def get_agg_select(aggregate,column):
    if aggregate == 'Count Distinct':
        return 'Count(Distinct %s)' % column
    else:
        return '%s(%s)' % (aggregate,column)

def get_selects(selected_columns,selected_aggregates):
    selects = selected_columns + [get_agg_select(aggregate,column) for aggregate,column in selected_aggregates]
    selects = ",".join(selects)
    
    return selects
    

def joins(join_table,join_columns):
    x = 'left join ' + join_table + ' on '
    for i,j in enumerate(join_columns):
        if i == 0:
            x += ('%s = %s' % (j[0],j[1]))
        else:
            x += (' and %s = %s' % (j[0],j[1]))
            
    return x

def get_filters(selected_filters):
    
    filter_map = {
        'equals': 'in',
        'does not equal': 'not in',
        'greater than': '>',
        'greater than or equal to': '>=',
        'less than': '<',
        'less than or equal to': '<=',
        'contains': 'ilike',
        'does not contain': 'not ilike',
        'is': 'is',
        'is not': 'is not'
        }
    
    if selected_filters:
        filters = []
        for filter_column, filter_operator, filter_value in selected_filters:
            if filter_operator in ['equals','does not equal']:
                filter_text = filter_column + ' ' + filter_map[filter_operator] + ' ' + '(' + ",".join(["'" + f + "'" for f in filter_value]) + ')'
            elif filter_operator in ['greater than','greater than or equal to','less than','less than or equal to']:
                filter_text = filter_column + ' ' + filter_map[filter_operator] + ' ' + str(filter_value)
            elif filter_operator in ['contains','does not contain']:
                filter_text = filter_column + ' ' + filter_map[filter_operator] + ' ' + "'" + str(filter_value) + "'"
            elif filter_value == 'blank':
                if filter_operator == 'is':
                    filter_text = filter_column + ' = ' + ''
                if filter_operator == 'is not':
                    filter_text = filter_column + ' != ' + ''
            else:
                filter_text = filter_column + ' ' + filter_map[filter_operator] + ' ' + filter_value
            filters.append(filter_text)
            
        for i,f in enumerate(filters):
            if i == 0:
                filters[i] = 'where ' + f
            else:
                filters[i] = 'and ' + f
                
        filters = " ".join(filters)
            
        return filters
    

def gen_query(tables,join_columns,selected_columns,selected_aggregates,selected_filters):
    
    sql_text = ''
    
    query_attributes = {
        'selects': get_selects(selected_columns,selected_aggregates),
        'tables': tables,
        'filters': get_filters(selected_filters),
        'groupbys': ",".join(str(i+1) for i in range(len(selected_columns)))
        }
    
    # return query_attributes
    
    if query_attributes['selects'] != '':
        for query_attribute in query_attributes.keys():
            if query_attribute == 'selects':
                sql_text += 'select %s from ' % query_attributes[query_attribute]
            if query_attribute == 'tables':
                if len(query_attributes[query_attribute]) == 1:
                    sql_text += tables[0] + ' '
                else:
                    sql_text += tables[0] + ' '
                    sql_text += joins(tables[1],join_columns) + ' '
            if query_attribute == 'filters' and query_attributes[query_attribute]:
                sql_text += query_attributes[query_attribute]
            if query_attribute == 'groupbys' and query_attributes[query_attribute] != '':
                sql_text += ' group by %s' % query_attributes[query_attribute]
    
    if sql_text != '':   
        return format_sql(sql_text)
        # return join_columns
    
    # sql_text +=
    
    # if len(tables) == 1:
    #     if len(selected_columns) > 0:
    #         sql_text = "select " + ",".join(selects) + ' from ' + tables[0] + " group by %s" % ",".join(str(i+1) for i in range(len(selected_columns)))
    #         sql_text = format_sql(sql_text)
    #     else:
    #         sql_text = "select " + ",".join(selects) + ' from ' + tables[0]
    #         sql_text = format_sql(sql_text)
        
    #     return sql_text
    # else:
    #     if len(selected_columns) > 0:
    #         sql_text = """select %s from %s %s group by %s""" % (
    #             ",".join(selects),
    #             tables[0],
    #             joins(tables[1],join_columns),
    #             ",".join(str(i+1) for i in range(len(selected_columns))))
    #     else:
    #         sql_text = """select %s from %s %s""" % (
    #             ",".join(selects),
    #             tables[0],
    #             joins(tables[1],join_columns))
    
    #     sql_text = format_sql(sql_text)            
        
    #     return sql_text
