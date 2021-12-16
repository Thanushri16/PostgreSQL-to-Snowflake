''' *********************************************************************************************************************

* Product: Snowflake Migration Platform

* Utility: "Mapper" which utilises the mapping input file to generate the datatype mapping of the PostgreSQL metatables 

* Date: Jun 2021

* Company: Dattendriya Data Science Solutions

************************************************************************************************************************* '''

import sqlite3
import pandas as pd
import re
import xlsxwriter
import os

#Get the path of the current working directory
path = os.getcwd()

#Read the excel spreadsheet containing the metadata tables and drop the duplicate values
meta = pd.read_excel(path+'\\psg_meta_info.xlsx', 'meta_views')
meta = meta[['table_name','column_name','data_type','character_maximum_length','numeric_precision','constraint_type','constraint_name']]
meta = meta.drop_duplicates()

#Connect to the sqlite database using meta_dmp as the database
conn = sqlite3.connect('psg_meta_dmp.db')
c = conn.cursor()

#Insert the values of a dataframe into the sql table META_TABLES
meta.to_sql('META_VIEWS', conn, if_exists='replace', index = False)  
c.execute('''SELECT * FROM META_VIEWS ''')
df = pd.DataFrame(c.fetchall(),columns = ['psg_table_name','psg_column_name','psg_data_type','psg_size','psg_numeric_precision','psg_constraint_type','psg_constraint_name'])
print(df) # To display the results after an insert query

#Read the mapping file and adding a new index column for easy access
datatype_map = pd.read_csv (path+'\\psg2snow_xlat.csv')
#datatype_map = datatype_map.dropna()
print(datatype_map)

#NEW METHOD: Convert datatype_map dataframe to a dictionary
#datatype_map.assign(PostgreSQL_Data_Type=datatype_map['PostgreSQL_Data_Type'].str.split(',')).explode('PostgreSQL_Data_Type')
#datatype_map = pd.DataFrame(datatype_map, columns=['PostgreSQL_Data_Type','Snowflake_Data_Type'])
datatype_map['PostgreSQL_Data_Type'] = datatype_map['PostgreSQL_Data_Type'].str.lower()
datatype_map['Snowflake_Data_Type'] = datatype_map['Snowflake_Data_Type'].str.lower()
datatype_map['PostgreSQL_Data_Type'] = datatype_map['PostgreSQL_Data_Type'].str.split('/')
datatype_map=datatype_map.explode('PostgreSQL_Data_Type')
datatype_map['PostgreSQL_Data_Type'] = datatype_map['PostgreSQL_Data_Type'].str.strip()
datatype_map['Snowflake_Data_Type'] = datatype_map['Snowflake_Data_Type'].str.strip()
datatype_map['idx'] = range(0, len(datatype_map))
datatype_map.set_index('idx',inplace = True) 
export_csv = datatype_map.to_csv (path+'\\temporary_datatype_map_views.csv', index = None, header=True)  #addednow
datatype_map_dict = dict(zip(datatype_map.PostgreSQL_Data_Type, datatype_map.Snowflake_Data_Type))
print(datatype_map_dict)

#The table_name, column_name columns are the same in postgres and snowflake
#So values from postgres are copied to snowflake columns
df['psg_data_type'] = df['psg_data_type'].str.lower()
df[['sflake_table_name']] = df[['psg_table_name']]
df[['sflake_column_name']] = df[['psg_column_name']]
df=pd.DataFrame(df)

#New Method: Mapping function
final_datatype_map = pd.read_csv (path+'\\temporary_datatype_map_views.csv')

#NEWLY ADDED
if (df['psg_data_type'].str.contains(r'\(([A-Za-z0-9_,]+)\)')).any():
    str_within_parentheses="("+df['psg_data_type'].str.extract(r'\(([A-Za-z0-9_,]+)\)')+")"
    str_parentheses = str_within_parentheses[str_within_parentheses[0].notnull()]
    index_str_parentheses = str_within_parentheses[str_within_parentheses[0].notnull()].index.tolist()
    
for i, (k, v) in enumerate(datatype_map_dict.items()):
    print(i, k, v)

    #NEWLY ADDED
    mod_dt = df['psg_data_type'].str.replace(r"\(([A-Za-z0-9_,]+)\)", '')     #Another pattern: r'\([^()]+\)'

    #pos = df[df['psg_data_type'] == k].index.tolist()
    #NEWLY ADDED
    pos = df[mod_dt == k].index.tolist()        #to match the input and the mapping dataset exactly
    print('Positions for ',k,': ',pos)
    df.loc[pos,['sflake_data_type']] = v
    df.loc[pos,['comments']] = final_datatype_map.loc[i,'Comments']

#NEWLY ADDED
if (df['psg_data_type'].str.contains(r'\(([A-Za-z0-9_,]+)\)')).any():
    print(str_within_parentheses)
    print(mod_dt)
    
#The size, constraint type and constraint name columns are the same in postgres and snowflake
#So values from postgres are copied to snowflake columns
df[['sflake_size']] = df[['psg_size']]
df[['sflake_numeric_precision']]=df[['psg_numeric_precision']]
df[['sflake_constraint_type']] = df[['psg_constraint_type']]
df[['sflake_constraint_name']] = df[['psg_constraint_name']]
print(df[['psg_data_type','sflake_data_type']].head(15))

#NEWLY ADDED
if (df['psg_data_type'].str.contains(r'\(([A-Za-z0-9_,]+)\)')).any():
    new_df=pd.DataFrame(df.loc[index_str_parentheses,'sflake_data_type'])
    print(new_df)
    str_parentheses=pd.DataFrame(str_parentheses)
    str_parentheses=str_parentheses.rename(columns = {0: 'precision'}, inplace = False)
    print(str_parentheses)
    newest_df=pd.DataFrame()
    newest_df=new_df.join(str_parentheses)
    newest_df['dt']=newest_df['sflake_data_type'].map(str) + newest_df['precision'].map(str)
    print(newest_df)
    df.loc[index_str_parentheses,'sflake_data_type']=newest_df.loc[index_str_parentheses,'dt']

#To get rows that have commments
format_cells_list = df[df['comments'].notnull()].index.tolist()
print(format_cells_list)

# To export the results to an excel file
writer = pd.ExcelWriter(path+'\\psg2snow_src_tgt_mapping_views.xlsx', engine='xlsxwriter')
export_excel = df.to_excel (writer, sheet_name = "source_target_mapping_views",index = None, header=True)
df.to_sql('psg2snow_src_tgt_mapping_views', conn, if_exists='replace', index = False)
print(pd.read_sql('SELECT * FROM psg2snow_src_tgt_mapping_views', conn))
conn.commit()
conn.close()

# Get workbook
workbook = writer.book
# Get Sheet
worksheet = writer.sheets['source_target_mapping_views']
# To format cells
cell_format1 = workbook.add_format({'bg_color': '#E3E4FA'})
cell_format1.set_border(1)

worksheet.set_column(0,6, 27, cell_format1)
cell_format2 = workbook.add_format({'bg_color': '#FFDFDD'})
cell_format2.set_border(1)
worksheet.set_column(7,14, 27, cell_format2)
cell_format3 = workbook.add_format({'font_color': 'red'})
cell_format3.set_border(1)
for i in format_cells_list:
    worksheet.set_row(i+1, None, cell_format3)
writer.close()
# To pop up the output excel file
os.startfile(path+'\\psg2snow_src_tgt_mapping_views.xlsx')
