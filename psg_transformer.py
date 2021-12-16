''' *************************************************************

* Product: Snowflake Migration Platform

* Utility: "Transformer" which generates Snowflake DDL script 

* Date: Jun 2021

* Company: Dattendriya Data Science Solutions

*************************************************************** '''

import sqlite3
import pandas as pd
import re
import xlsxwriter
import os

#Get the path of the current working directory
path = os.getcwd()             

#Read the csv file and drop the duplicate values
map_data = pd.read_excel (path+'\\psg2snow_src_tgt_mapping.xlsx')
map_data = map_data[['sflake_table_name','sflake_column_name','sflake_data_type','sflake_size','sflake_precision','sflake_isnullable','comments','sflake_constraint_type','sflake_constraint_name']]
map_data = map_data.drop_duplicates()

df = pd.DataFrame(map_data,columns=['sflake_table_name','sflake_column_name','sflake_data_type','sflake_size','sflake_precision','sflake_isnullable','sflake_constraint_type','sflake_constraint_name','comments'])

df_without_duplicates_inter=df.copy(deep=True)
df_without_duplicates=df.copy(deep=True)
#df_without_duplicates=df_without_duplicates_inter
#print(df_without_duplicates)
df1=df_without_duplicates_inter[df_without_duplicates_inter.duplicated(['sflake_table_name','sflake_column_name','sflake_data_type','sflake_size','sflake_precision','sflake_isnullable','comments'],keep=False)]
indices=df1[df1['sflake_constraint_type']=='foreign key'].index.tolist()
print(indices)
rows = df_without_duplicates.index[indices]
df_without_duplicates.drop(rows, inplace=True)
print(df_without_duplicates)

#df1=df.loc[df['sflake_constraint_type']!='foreign key']
#print(df1)
df2=df.loc[df['sflake_constraint_type']=='foreign key']
print(df2)

#Connect to the sqlite database using mapping_dmp as the database
conn1 = sqlite3.connect('psg_mapping_dmp.db')
c1 = conn1.cursor()

#Insert the values of the dataframes into the sql tables SNOWFLAKE_TABLE and SNOWFLAKE_FOREIGN_TABLE
df_without_duplicates.to_sql('SNOWFLAKE_TABLE', conn1, if_exists='replace', index = False)
l=c1.execute('''SELECT distinct(sflake_table_name) FROM SNOWFLAKE_TABLE; ''').fetchall()
l = [ i for i, in l ]
print('Distinct tables:',l)
df2.to_sql('SNOWFLAKE_FOREIGN_TABLE', conn1, if_exists='replace', index = False)
l2=c1.execute('''SELECT distinct(sflake_table_name) FROM SNOWFLAKE_FOREIGN_TABLE; ''').fetchall()
l2 = [ i for i, in l2 ]

cons_list=[]
#NOT NULL
notnull_list=[]
#To print the header in the script file
t = "/* *************************************************************\n\n"
t = t + "* Product: Snowflake Migration Platform\n\n"
t = t + '* Utility: "Transformer" which generates Snowflake DDL script\n\n'
t = t + "* Date: Jun 2021\n\n"
t = t + "* Company: Dattendriya Data Science Solutions\n\n"
t = t + "*************************************************************** */\n\n"

#Database and Schema creation
t = t + "--Database Created\n"
t = t + "CREATE OR REPLACE DATABASE PSG_META_DVDRENTAL_PUBLIC;\n\n"
t = t + "--Schema Created\n"
t = t + "USE PSG_META_DVDRENTAL_PUBLIC.PUBLIC;\n\n"
with open(path+'\\psg_snowflake_ddl.sql', 'a') as file:
    file.write(t)
file.close()

#Create table command script creation
for table in l:
    t="--TABLE:"+table+"\n"
    t=t+"CREATE OR REPLACE TABLE "+table+" ( "
    t=t+"\n"
    res=c1.execute('''SELECT sflake_table_name,sflake_column_name,sflake_data_type,sflake_size,sflake_precision,sflake_isnullable,sflake_constraint_type,sflake_constraint_name,comments FROM SNOWFLAKE_TABLE where sflake_table_name = "'''+table+'''"; ''').fetchall()
    res = [[tn,cn,dt,ds,p,n,ct,con,com] for (tn,cn,dt,ds,p,n,ct,con,com) in res]
    noofcolumns=len(res)        #to remove the comma following the last column creation line
    #print(res)
    
    for valueset in res:
        #if valueset[6] != None:
            #t=t+"--"+valueset[6]+"\n"   #Adding commands for the alerts and the warning in the mapping_output table
        idx=0
        for i in valueset:
            new_i=i
            #idx=valueset.index(i)
            if i==valueset[0]:
                if i==valueset[1] and idx==0:
                    new_i=i
                    idx=idx+1
                else:
                    continue
            
            if i==None:           #to replace all the none type values in sizes to ''
                continue
                #i=''
            
            if bool(i==valueset[6]) | bool(i==valueset[7]):
                cons_list.append(tuple(valueset))   #add the rows which contain constraints in a set to be used in the alter command later
                continue
                #i=''
            
            #CHANGED: to check for sizes
            if i==valueset[3]:
                if type(i)==float:
                    new_i="("+str(int(i))+")"
                if type(i)==int:
                    new_i="("+str(i)+")"
                if i==int(0):             #if datatype size is 0, then dont add it in parentheses next to datatype.
                    continue

            #CHANGED: add precision
            if i==valueset[4] and i!=None:
                continue

            #NOT NULL
            if i == valueset[5]:
                if i=='no':
                    new_i="NOT NULL"   #add the rows which contain not null in a set to be used in the alter command later
                else:
                    continue
            
            #if type(i)!=float and len(i.split(", "))==2:
                #new_i=i.split(", ")[0]
                
            if i==valueset[8] and "ALERT" in valueset[8]:
                if valueset != res[noofcolumns-1]:
                    new_i=", --**"+valueset[8]+"**"
                else:
                    new_i=" --**"+valueset[8]+"**"
                    
            if i==valueset[8] and "WARNING" in valueset[8]:
                if valueset != res[noofcolumns-1]:
                    new_i=", --^^"+valueset[8]+"^^"
                else:
                    new_i=" --^^"+valueset[8]+"^^"
                    
            if i!=valueset[8]:
                t=t+' '+new_i
            else:
                t=t+new_i
                
        if valueset != res[noofcolumns-1]:    #to remove the comma following the last column creation line
            t=t+","
        t=t+"\n"
        
    t=t+" );"
    t=t+"\n\n"
    #to write the script to a .sql file
    with open(path+'\\psg_snowflake_ddl.sql', 'a') as file:
        file.write(t)
    file.close()
    
#Choosing only primary key constraints from other constraints
#pk_cons_set=set(cons_list)
#print(pk_cons_set)
#print(len(pk_cons_set))  
cons_set=set(cons_list)
print(cons_set)
print(len(cons_set))
pk_cons_list=[]
for valueset in cons_list:
    if valueset[6]=='primary key':                  #CHANGED: change 4 to 5 when precision is added
        pk_cons_list.append(tuple(valueset))
pk_cons_set=set(pk_cons_list)
print(pk_cons_set)
print(len(pk_cons_set))


#Adding the primary key constraints to the file
p='--Adding Primary Key constraints\n'
data=pd.DataFrame(pk_cons_set,columns=['table','column','datatype','size','precision','isnullable','constrainttype','constraintname','comments']).sort_values(by = 'table')
data=data.reset_index(drop=True)
#print(data)
#pos = data[data['table'].duplicated(keep=False)].index.tolist()
new_data=data[data['table'].duplicated(keep=False)]
uniquetables=new_data['table'].unique()
cond = data['table'].isin(new_data['table'])
data.drop(data[cond].index, inplace = True)
#print(data)
new_pk_list=[]
for ind in data.index:
     x=(data['table'][ind],data['column'][ind],data['datatype'][ind],data['size'][ind],data['precision'][ind],data['isnullable'][ind],data['constrainttype'][ind],data['constraintname'][ind],data['comments'][ind])
     new_pk_list.append(x)

#Looking for composite primary keys
for i in uniquetables:
    indexval=new_data[new_data['table']==i].index.tolist()
    val=','.join(list(new_data.loc[indexval,'column']))
    tab=i
    constraint='primary key'
    constraint_name=set(list(new_data[new_data['table']==i]['constraintname']))
    cons_name=[str(s) for s in constraint_name]
    name=''.join(cons_name)
    x=(tab,val,None,None,None,None,constraint,name,None)
    new_pk_list.append(x)
print(new_pk_list)
print(len(new_pk_list))

for row in sorted(set(new_pk_list)):
    if row[6]=='primary key':
        p=p+"ALTER TABLE "+row[0]+" ADD CONSTRAINT "+row[7]+" "+row[6]+" ("+row[1]+");\n"

#UNIQUE
#Choosing only UNIQUE constraints from other constraints
uni_cons_list=[]
for valueset in cons_list:
    if valueset[6]=='unique':                  #CHANGED: change 4 to 5 when precision is added
        uni_cons_list.append(tuple(valueset))
uni_cons_set=set(uni_cons_list)
print(uni_cons_set)
print(len(uni_cons_set))

#UNIQUE
#Adding UNIQUE constraints to the file
u='\n--Adding unique constraints\n'
data=pd.DataFrame(uni_cons_set,columns=['table','column','datatype','size','precision','isnullable','constrainttype','constraintname','comments']).sort_values(by = 'table')
data=data.reset_index(drop=True)
#print(data)
#pos = data[data['table'].duplicated(keep=False)].index.tolist()
new_data=data[data['table'].duplicated(keep=False)]
uniquetables=new_data['table'].unique()
cond = data['table'].isin(new_data['table'])
data.drop(data[cond].index, inplace = True)
#print(data)
new_uni_list=[]
for ind in data.index:
     x=(data['table'][ind],data['column'][ind],data['datatype'][ind],data['size'][ind],data['precision'][ind],data['isnullable'][ind],data['constrainttype'][ind],data['constraintname'][ind],data['comments'][ind])
     new_uni_list.append(x)

#Looking for composite unique keys
for i in uniquetables:
    indexval=new_data[new_data['table']==i].index.tolist()
    val=','.join(list(new_data.loc[indexval,'column']))
    tab=i
    constraint='unique'
    constraint_name=set(list(new_data[new_data['table']==i]['constraintname']))
    cons_name=[str(s) for s in constraint_name]
    name=''.join(cons_name)
    x=(tab,val,None,None,None,None,constraint,name,None)
    new_uni_list.append(x)
print(new_uni_list)
print(len(new_uni_list))

for row in sorted(set(new_uni_list)):
    if row[6]=='unique':
        u=u+"ALTER TABLE "+row[0]+" ADD CONSTRAINT "+row[7]+" "+row[6]+" ("+row[1]+");\n"

fk_cons_list=[]
#To append the valuesets with foreign key constraints to be added in the alter table command later
foreignvaluesets=c1.execute('''SELECT sflake_table_name,sflake_column_name,sflake_data_type,sflake_size,sflake_precision,sflake_isnullable,sflake_constraint_type,sflake_constraint_name FROM SNOWFLAKE_FOREIGN_TABLE ''').fetchall()
foreignvaluesets = [[tn,cn,dt,ds,p,n,consn,ct] for (tn,cn,dt,ds,p,n,consn,ct) in foreignvaluesets]
for valueset in foreignvaluesets:
    for i in valueset:
        if i=='foreign key':
            fk_cons_list.append(tuple(valueset))                   #add the rows which contain constraints in a set to be used in the alter command later
            
#Sets of values with constraints
fk_cons_set=set(fk_cons_list)
print(fk_cons_set)
print(len(fk_cons_set))    

#To generate alter table add constraint commands 
table_dep = pd.read_excel (path+'\\table_dependency.xlsx')

#Connecting the table_dependency table with the existing table to get details of the foreign table and its columns
#TABLE_DEPENDENCY as a table in sqlite
table_dep.to_sql('TABLE_DEPENDENCY', conn1, if_exists='replace', index = False)
print(pd.read_sql('SELECT * from TABLE_DEPENDENCY',conn1))
l3=c1.execute('''SELECT td.foreign_column_name,td.foreign_table_name,td.table_name,td.column_name FROM TABLE_DEPENDENCY td JOIN SNOWFLAKE_FOREIGN_TABLE sft ON td.foreign_table_name = sft.sflake_table_name AND td.foreign_column_name = sft.sflake_column_name; ''').fetchall()
final_list = list(set(l3))
final_list=[[h,i,j,k] for (h,i,j,k) in final_list]
print(final_list)
print(len(final_list))

#Adding foreign key constraints to the file
f='\n--Adding Foreign Key constraints\n'
for row in sorted(fk_cons_set):
    table_to_be_matched = row[0]
    column_to_be_matched = row[1]
        #foreigntabledetails=[]
    for line in final_list:
        if (line[1] == table_to_be_matched) and (line[0] == column_to_be_matched):
            if(line[2] in row[7]):
                    #foreigntabledetails.append(line[2],row[5],row[4],row[1],row[0],row[1])
                f=f+"ALTER TABLE "+line[2]+" ADD CONSTRAINT "+row[7]+" "+row[6]+" ("+line[3]+") references "+row[0]+" ("+row[1]+");\n"
        
with open(path+'\\psg_snowflake_ddl.sql', 'a') as file:
    file.write(p)
    #UNIQUE
    file.write(u)
    file.write(f)
file.close()
