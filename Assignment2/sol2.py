#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################
NO_OF_THREADS = 5

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    #pass #Remove this once you are done with implementation
    cur = openconnection.cursor()
    temp = "SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + ";"
    cur.execute(temp)
    minval = cur.fetchone()[0]

    temp = "SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + ";"
    cur.execute(temp)
    maxval = cur.fetchone()[0]

    interval = float (maxval-minval) / NO_OF_THREADS

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable + "\';"
    cur.execute(temp)
    InputTableSchema = cur.fetchall()

    for i in range(NO_OF_THREADS):
        tblName = "range_part" + str(i)
        temp = "CREATE TABLE " + tblName + " (" + InputTableSchema[0][0] + " " + InputTableSchema[0][1] + ");"
        cur.execute(temp)
        for each in range(1,len(InputTableSchema)):
            temp = "ALTER TABLE " + tblName + " ADD COLUMN " + InputTableSchema[each][0] + " " + InputTableSchema[each][1] + ";"
            cur.execute(temp)
    threads = [0,0,0,0,0]
    for i in range(NO_OF_THREADS):
        if i == 0:
            minvalue = minval
            maxvalue = minvalue + interval
        else:
            minvalue = maxvalue
            maxvalue = maxvalue + interval

        threads[i] = threading.Thread(target=Sorted,args=(InputTable,SortingColumnName,i,minvalue,maxvalue,openconnection))
        threads[i].start()

    for i in range(NO_OF_THREADS):
        threads[i].join()

    temp = "CREATE TABLE " + OutputTable + " (" + InputTableSchema[0][0] + " INTEGER);"
    cur.execute(temp)
    for each in range(1,len(InputTableSchema)):
            temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema[each][0] + " " + InputTableSchema[each][1] + ";"
            cur.execute(temp)
    for i in range(NO_OF_THREADS):
        temp = "INSERT INTO " + OutputTable + " SELECT * FROM range_part" + str(i) + ";"
        cur.execute(temp)

    for i in range(NO_OF_THREADS):
        temp = "DROP TABLE IF EXISTS range_part" + str(i) + ";"
        cur.execute(temp)

    openconnection.commit()         

def Sorted(InputTable,SortingColumnName,i,minvalue,maxvalue,openconnection):
    cur = openconnection.cursor()
    tblName = "range_part" + str(i)
    if i == 0:
        temp = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(minvalue) + " AND " + SortingColumnName + " <= " + str(maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"
    else:
        temp = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(minvalue) + " AND " + SortingColumnName + " <= " + str(maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"      
    cur.execute(temp)
    return

    
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation
    cur = openconnection.cursor()
    temp = "SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cur.execute(temp)
    mintbl1 = float(cur.fetchone()[0])

    temp = "SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cur.execute(temp)
    mintbl2 = float(cur.fetchone()[0])

    temp = "SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cur.execute(temp)
    maxtbl1 = float(cur.fetchone()[0])

    temp = "SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cur.execute(temp)
    maxtbl2 = float(cur.fetchone()[0])

    maxtbl = max(maxtbl1,maxtbl2)
    mintbl = min(mintbl1,mintbl2)
    interval = (maxtbl - mintbl) / NO_OF_THREADS

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable1 + "\';"
    cur.execute(temp)
    InputTableSchema1 = cur.fetchall()

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable2 + "\';"
    cur.execute(temp)
    InputTableSchema2 = cur.fetchall()

    temp = "CREATE TABLE " + OutputTable + " (" + InputTableSchema1[0][0] + " INTEGER);"
    cur.execute(temp)

    for each in range(1,len(InputTableSchema1)):
        temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema1[each][0] + " " + InputTableSchema1[each][1] + ";"
        cur.execute(temp)

    for each in range(len(InputTableSchema2)):
        temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema2[each][0] + " " + InputTableSchema2[each][1] + ";"
        cur.execute(temp)    

    for i in range(NO_OF_THREADS):
        tblName = "inputtable1_" + str(i)
        if i == 0:
            minvalue = mintbl
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(minvalue) + " AND " + Table1JoinColumn + " <= " + str(maxvalue)  + ";"
        else:
            minvalue = maxvalue
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(minvalue) + " AND " + Table1JoinColumn + " <= " + str(maxvalue)  + ";"
        cur.execute(temp)
    
    for i in range(NO_OF_THREADS):
        tblName = "inputtable2_" + str(i)
        if i == 0:
            minvalue = mintbl
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(minvalue) + " AND " + Table2JoinColumn + " <= " + str(maxvalue) + ";"
        else:
            minvalue = maxvalue
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(minvalue) + " AND " + Table2JoinColumn + " <= " + str(maxvalue) + ";"
        cur.execute(temp)

    for i in range(NO_OF_THREADS):
        OutputTableRange = "outtable_range" + str(i)
        temp = "CREATE TABLE " + OutputTableRange + " (" + InputTableSchema1[0][0] + " INTEGER);"
        cur.execute(temp)

        for each in range(1,len(InputTableSchema1)):
            temp = "ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema1[each][0] + " " + InputTableSchema1[each][1] + ";"
            cur.execute(temp)

        for each in range(len(InputTableSchema2)):
            temp = "ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema2[each][0] + " " + InputTableSchema2[each][1] + ";"
            cur.execute(temp)

    threads = [0,0,0,0,0]
    for i in range(NO_OF_THREADS):
        threads[i] = threading.Thread(target=Join,args=(Table1JoinColumn,Table2JoinColumn,openconnection,i))
        threads[i].start()

    for i in range(NO_OF_THREADS):
        threads[i].join()

    for i in range(NO_OF_THREADS):
        temp = "INSERT INTO " + OutputTable + " SELECT * FROM outtable_range" + str(i) + ";"
        cur.execute(temp)

    for i in range(NO_OF_THREADS):
        temp = "DROP TABLE IF EXISTS inputtable1_" + str(i) + ";" 
        temp1 = "DROP TABLE IF EXISTS inputtable2_" + str(i) + ";" 
        temp2 = "DROP TABLE IF EXISTS outtable_range" + str(i) + ";" 
        cur.execute(temp)   
        cur.execute(temp1) 
        cur.execute(temp2) 

    openconnection.commit()

def Join(Table1JoinColumn,Table2JoinColumn,openconnection,i):
    cur = openconnection.cursor()
    temp = """INSERT INTO outtable_range""" + str(i) + """ SELECT * FROM inputtable1_""" + str(i) + """ INNER JOIN inputtable2_""" + str(i) +""" ON inputtable1_""" + str(i) + """.""" + str(Table1JoinColumn).lower() + """ = inputtable2_""" + str(i) + """.""" + str(Table2JoinColumn).lower() + """;"""
    cur.execute(temp)
    return 
