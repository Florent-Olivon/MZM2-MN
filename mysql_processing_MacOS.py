import pymysql
import os
import sys
import struct
import time


import bootstrap

mamp_ip_adress = bootstrap.mamp_ip_adress
mamp_port = bootstrap.mamp_port
user_account_name = bootstrap.user_account_name

database_name = bootstrap.database_name
table_name = bootstrap.table_name


###########################################################################
############################# Database connection/creation ################
###########################################################################

if user_account_name == "root":
    print("Please change the default 'user' line in the bootstrap.py file and start the script again")
    x = input("Press Enter to continue")
    sys.exit()
user_password = input("Enter password for {} : ".format(user_account_name))
#########################""
## Check connection statut
## with Wamp

try:
    connector = pymysql.connect(host=mamp_ip_adress, user=user_account_name, password=user_password, port=mamp_port)
    cursor = connector.cursor()
    print("Connection to Mamp : Successfull") 
except:
    print("Error, could not connect to the SQL database. Please check connection parameters and/or start MampServer again.")
    x = input("Press Enter to continue")
    sys.exit()

########################################
## Try to connect to "default_database"
## and clear the "default_table".
## If not, create the table

try:
    connector = pymysql.connect(host=mamp_ip_adress, user=user_account_name, password=user_password, db=database_name, port=mamp_port)
    cursor = connector.cursor()
    print("Connection to the Database : Successful")
    cursor.execute("TRUNCATE TABLE {}".format(table_name))
except:       
    try:
        cursor.execute("DROP DATABASE {}".format(database_name))
    except:
        pass   
    sql_request = "CREATE DATABASE {} DEFAULT CHARACTER SET utf8;".format(database_name)    
    cursor.execute(sql_request)
    connector = pymysql.connect(host=mamp_ip_adress, user=user_account_name, password=user_password, db=database_name, port=mamp_port)
    cursor = connector.cursor()
    sql_requests = ["CREATE TABLE {} (ID int NOT NULL)".format(table_name)]
    sql_requests.append("ALTER TABLE {} ADD mz double NOT NULL".format(table_name))
    sql_requests.append("ALTER TABLE {} ADD MS2 blob".format(table_name))
    sql_requests.append("ALTER TABLE {} ADD RT double".format(table_name))
    sql_requests.append("ALTER TABLE {} ADD FileName char(150)".format(table_name))
    sql_requests.append("ALTER TABLE {} ADD Area double".format(table_name))
    sql_requests.append("ALTER TABLE {} ADD Formula char(75)".format(table_name))
    sql_requests.append("ALTER TABLE {} ADD Comment char(250)".format(table_name))
    try:
        for request in sql_requests:
            cursor.execute(request)
        print("Table creation : Successful")
    except:
        print("Error in Table creation at {}".format(request))
        pass
    

print("#######################################################")
print("")
print("Please export your prossessed data from MzMine2")
print("Use these parameters")
print("JDBC connection string : jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(mamp_ip_adress, mamp_port, database_name, user_account_name, user_password))
print("Database table : {}".format(table_name))
import pandas as pd
x = input("Press Enter to continue")



########################################################################
############################## Creation of the .MGF file ###############
########################################################################



#################################
## Getting data from the database
## e.g. : ID, mz and MS2
## And creation of the new_mgf_file

sql_request = 'SELECT ID, mz, MS2 FROM {}'.format(table_name)
cursor.execute(sql_request)

if cursor.execute(sql_request) == 0:
    print("")
    print("Error : Database is empty. Please try again.")
    x = input("Press Enter to continue")
    sys.exit()


new_mgf_file = open('new_mgf_file.mgf', "w")

#################################
## Getting throught the data and
## writing into the file

list_IDs = []
print("Exporting MGF file, please wait.")
for row in cursor:
        if row[0] not in list_IDs:
                binary_content = row[2]
                if binary_content is not None:
                        lines_to_write = []
                        list_IDs.append(row[0])
                        lines_to_write.append("BEGIN IONS")
                        lines_to_write.append("PEPMASS={}".format(row[1]))
                        lines_to_write.append("TITLE=MzMine MS/MS of {0} a+ at {1} mins".format(row[1], row[0]))
                        lines_to_write.append("RTINSECONDS={}".format(row[0]))

                        newFile = open("tempfile.txt", "wb")
                        newFile.write(binary_content)
                        newFile.close()
                        temp_file = open("tempfile.txt", "rb")
                        s = struct.Struct(">dd")
                        while True:
                                byte = temp_file.read(16);
                                if len(byte) != 16:
                                        break;
                                data_couple = s.unpack(byte)
                                lines_to_write.append("{0}\t{1}".format(data_couple[0], data_couple[1]))
                        lines_to_write.append("END IONS")
                        temp_file.close()
                        new_mgf_file.write('\n'.join(lines_to_write)+ '\n')
new_mgf_file.close()
os.remove('tempfile.txt')

print("#######################################################")
print("")
print("MGF file creation : Successful")




###############################################################################
############################# Creation of the .XLS file #######################
###############################################################################

print("Exporting XLS file, please wait.")
csv_file = 'new_xls_file.xls'


###################################
## Stocking columns list from the 
## SQL database into 'columns_list'
## Each column_info contains a list
## of ColumnName, ColumnType, Ability
## to be a Null.

query = 'SHOW columns FROM {}'.format(table_name)
cursor.execute(query)

columns_list = []
for column_info in cursor:
    columns_list.append(column_info[0])

###################################
## Getting data from the MySQL database
## and stocking into 'raw_data'
## Binary file contained in BLOBs
## are excluded from the 'raw_data'
## database and replaced by None value.

query = 'SELECT * FROM {}'.format(table_name)
cursor.execute(query)

raw_data, formatted_row = [], []

for row in cursor:
    formatted_row = [row[0], round(row[1], 6), None, round(row[3], 4), row[4], round(row[5]), row[6], row[7]]
    if row[2] is not None:
        raw_data.append(formatted_row)
        


####################################
## Creating a data frame containing
## the raw data excluding MS2 info

raw_df = pd.DataFrame(raw_data, columns=columns_list)


####################################
## Getting IDs from raw_data['ID'] and
## storing in 'list_id'
## Getting each sample name and storing
## in 'list_sample'

list_id = raw_df['ID']
list_id = list(set(list_id))

list_sample = raw_df['FileName']
list_sample = sorted(list(set(list_sample)))


####################################
## Creating a new list of columns for
## the new data frame as 'columns_list_new'
## containing Retention Time, m/z,
## guessed Formula and a column for
## each sample detected for area comparison

columns_list_new = ['RT', 'mz', 'Formula', 'Comment']

for sample in list_sample:
    columns_list_new.append(sample)

#####################################
## Creating an empty new data frame
## as new_df
    
new_df = pd.DataFrame(index=list_id, columns=columns_list_new)


####################################
## Getting for each ID the corresponding
## retention time, m/z and formula 
## storing them as tuples into 'formatted_info'
##
## Getting for each ID the corresponding
## sample name and area and storing them
## as tuples into list_areas

formatted_info = []
list_areas = []

for row in raw_df.index:
    formatted_info.append((raw_df.loc[row, 'ID'], raw_df.loc[row, 'RT'], raw_df.loc[row, 'mz'], raw_df.loc[row, 'Formula'], raw_df.loc[row, 'Comment']))
    list_areas.append((raw_df.loc[row,'ID'], raw_df.loc[row, 'FileName'], raw_df.loc[row, 'Area']))

formatted_info = set(formatted_info)
list_areas = set(list_areas)



######################################
## Filling the new data frame

for item in formatted_info:
    new_df.loc[item[0], 'RT'] = item[1]
    new_df.loc[item[0], 'mz'] = item[2]
    new_df.loc[item[0], 'Formula'] = item[3]
    new_df.loc[item[0], 'Comment'] = item[4]

for item in list_areas:
    new_df.loc[item[0], item[1]] = item[2]

#######################################
## Finding missing data and replacing
## empty areas by 0 and empty formulas
## by '-'
    
for item in new_df.index:
    if pd.isnull(new_df.loc[item, 'Formula']):
        new_df.loc[item, 'Formula'] = '-'

for sample in list_sample:
    for item in new_df.index:
        if pd.isnull(new_df.loc[item, sample]):
            new_df.loc[item, sample] = 0



#########################################
## Exporting the new data frame as an .xls
## file

new_df.index.name = "ID"
new_df.to_excel(csv_file, index=True)
print("XLS file creation : successful")
print("")
print("#######################################################")
print("")
print("Clearing database : successful")
print("")
print("#######################################################")
sql_request  = "DROP DATABASE {}".format(database_name)
cursor.execute(sql_request)


###############################################################################
############################# Adding group mapping ############################
###############################################################################




answer = ""
while answer != "y" and answer != "n":
    print("Do you want to add a Group Mapping file ? y/n")
    answer = input("Your answer : ")

if answer == "n":
    print("End of the script.")
    x = input("Press Enter to exit")
    sys.exit()

#####################################
## If a group mapping is wanted
## check for present files

import re
files_list = []
i = 0

print("Please be sure that a group mapping file is present in the current directory and press any key to continue.")
x = input("Press Enter to continue")

for file in os.listdir():
    if ".txt" in str(file):
        files_list.append([i, file])
        i += 1

print("")
print("Please select one of the following files :")
for file in files_list:
    print(str(file[0]) + " = " + str(file[1])) 

file_selection = input("File number : ")


check_number = False
for file in files_list:
    if str(file_selection) == str(file[0]):
        check_number = True
        selected_file_name = file[1]
        
while check_number is False:
    print("This number is not in the list")
    print("Please select one of the following files :")
    for file in files_list:
        print(str(file[0]) + " = " + str(file[1])) 
    
    
    file_selection = input("File number : ")
    for file in files_list:
        if str(file_selection) == str(file[0]):
            check_number = True
            selected_file_name = file[1]
            
file = open(selected_file_name, "r")


## Group names can contain = a-z, A-Z, 0-9, -, +, spaces, _, /, \, :
group_list = []
for line in file:
    if line.startswith("GROUP_"):
        group_name = re.findall("GROUP_([a-zA-Z0-9-\s+_\/\.:\\\[\]']+)", line)        
        grouped_files = re.findall("GROUP_[a-zA-Z0-9-\s+_\/\.:\\\[\]']+=([a-zA-Z0-9-+_\/\.:\\\[\]';]+)", line)
        
        grouped_files = re.findall("([a-zA-Z0-9-\s+_\/\.:\\\[\]']+)", grouped_files[0])
        group_list.append([group_name[0], grouped_files])

###################################
## Detection of present files in the
## group mapping file and deleting
## doubles
def clean_group_mapping(analysis_samples, mapping_groups):
    to_process_groups = []
    for group in mapping_groups:
        for sample in analysis_samples:
                if sample in group[1]:
                    to_process_groups.append(group)
    
    to_return_group_list = {}
    for group in to_process_groups:
        temp_list = []
        for group_file in group[1]:
            if group_file in analysis_samples:
                temp_list.append(group_file)
            to_return_group_list[group[0]] = temp_list
    return to_return_group_list

present_groups = clean_group_mapping(list_sample, group_list)


#######################################
## Adding new columns into the database

sum_list = []
for group in present_groups:
    for IDs in new_df.index:
        value = 0
        for file in present_groups[group]:
            value = value + new_df.loc[IDs, file]
        new_df.loc[IDs, group] = value
    
    
new_df.to_excel(csv_file, index=True)

print("")
print("Group mapping column have been added to the CSV file.")
print("End of the script, thanks for using it!")
input("Press Enter to exit")
