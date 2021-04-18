#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    conn = openconnection
    cur = conn.cursor()

    #get min and max values in SortingColumnName
    minVal = getMin(openconnection, SortingColumnName, InputTable)
    maxVal = getMax(openconnection, SortingColumnName, InputTable)

    numThreads = 5          # 5 threads to be used as specified in assignment
    threadList = []         # list to store thread objects
    tableNamePrefix = "parallelSort_part_" # prefix for partition table that needs to be created 
    rangeVal = float((maxVal-minVal)/numThreads) # range of ratings each partition will store and sort

    for i in range(numThreads):
        #min and max value of rating for thread[i]
        minValPart = minVal + (i*rangeVal)
        maxValPart = minValPart + rangeVal
        
        #thread constructor
        t = threading.Thread(target=createSortTablePartition, args=(openconnection, InputTable, SortingColumnName, i, minValPart, maxValPart, tableNamePrefix))
        threadList.append(t)
        t.start() #thread calls createSortTablePartition()
    
    for i in range(numThreads):
        #wait for all threads to complete
        threadList[i].join()
    
    #create OutputTable LIKE InputTable
    createNewTable(openconnection, OutputTable, InputTable)

    for i in range(numThreads):
        #insert rows in output table in order partTable0, partTable1, partTable2....
        insertSortPartitionQuery = "INSERT INTO {0} SELECT * FROM {1}{2};".format(OutputTable, tableNamePrefix, i)
        cur.execute(insertSortPartitionQuery)
    
    for i in range(numThreads):
        #delete temporary part tables that we created for parallel sorting
        dropSortPartTableQuery = "DROP TABLE IF EXISTS {0}{1};".format(tableNamePrefix,i)
        cur.execute(dropSortPartTableQuery)

    conn.commit()


def createSortTablePartition(openconnection, inputTable, sortingColumnName, i, minVal, maxVal, prefix):
    conn = openconnection
    cur = conn.cursor()
    
    #drop any previous partition table that may exist from other test cases
    dropSortPartTableQuery = "DROP TABLE IF EXISTS {0}{1};".format(prefix,i)
    cur.execute(dropSortPartTableQuery)
    
    #create partition table with same schema as input table 
    createPartTableQuery = "CREATE TABLE {0}{1} (LIKE {2} INCLUDING ALL);".format(prefix, i, inputTable)
    cur.execute(createPartTableQuery)
    
    if i==0:
        #because first sort range should be [minVal, maxVal]
        sortInsertQuery = "INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {3} <= {5} ORDER BY {3} ASC;".format(prefix, i, inputTable, sortingColumnName, minVal, maxVal)
    else:
        #other sort ranges should be (minVal, maxVal] to avoid duplicate rows being inserted
        sortInsertQuery = "INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} > {4} AND {3} <= {5} ORDER BY {3} ASC;".format(prefix, i, inputTable, sortingColumnName, minVal, maxVal)
    cur.execute(sortInsertQuery)

    conn.commit()



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    conn = openconnection
    cur = conn.cursor()

    minValTable1 = getMin(openconnection, Table1JoinColumn, InputTable1)
    maxValTable1 = getMax(openconnection, Table1JoinColumn, InputTable1)

    minValTable2 = getMin(openconnection, Table2JoinColumn, InputTable2)
    maxValTable2 = getMax(openconnection, Table2JoinColumn, InputTable2)

    #get min and max values across Table1 and Table2
    minVal = min(minValTable1, minValTable2)
    maxVal = max(maxValTable1, maxValTable2)
    
    # OutputTable schema = InputTable1 schema
    createNewTable(openconnection, OutputTable, InputTable1)

    # OutputTable schema = InputTable1 schema + InputTable2 schema
    schema_InputTable2 = getSchema(openconnection, InputTable2)

    for i in range(len(schema_InputTable2)):
        alterTableQuery = "ALTER TABLE {0} ADD COLUMN {1} {2};".format(OutputTable, schema_InputTable2[i][0], schema_InputTable2[i][1])
        cur.execute(alterTableQuery)
    
    numThreads = 5          # 5 threads to be used
    threadList = []         # list to store thread objects
    tableNamePrefix = "_join_part_" # prefix for partition table that needs to be created 
    rangeVal = float((maxVal-minVal)/numThreads) # range of joinColumn values each partition will store

    for i in range(numThreads):
        #min and max value of creating partition for thread[i]
        minValPart = minVal + (i*rangeVal)
        maxValPart = minValPart + rangeVal

        #thread constructor
        t = threading.Thread(target=createJoinTablePartition, args=(openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, i, minValPart, maxValPart, tableNamePrefix))
        threadList.append(t)
        t.start() #thread calls createJoinTablePartition()

    for i in range(numThreads):
        #main thread waits for all thread to complete task
        threadList[i].join()
    
    for i in range(numThreads):
        #insert rows in output table in order join_part_0, join_part_1, join_part_2....
        insertJoinPartitionQuery = "INSERT INTO {0} SELECT * FROM {0}{1}{2};".format(OutputTable, tableNamePrefix, i)
        cur.execute(insertJoinPartitionQuery)
    
    for i in range(numThreads):
        #delete all partitions we created for parallel join operation
        dropPartTable1Query = "DROP TABLE IF EXISTS {0}{1}{2};".format(InputTable1,tableNamePrefix,i)
        cur.execute(dropPartTable1Query)

        dropPartTable2Query = "DROP TABLE IF EXISTS {0}{1}{2};".format(InputTable2,tableNamePrefix,i)
        cur.execute(dropPartTable2Query)

        dropPartOutputQuery = "DROP TABLE IF EXISTS {0}{1}{2};".format(OutputTable,tableNamePrefix,i)
        cur.execute(dropPartOutputQuery)

    conn.commit()


def createJoinTablePartition(openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, i, minValPart, maxValPart, tableNamePrefix):
    conn = openconnection	
    cur = conn.cursor()

    table1_part = str(InputTable1)+str(tableNamePrefix)+str(i)
    table2_part = str(InputTable2)+str(tableNamePrefix)+str(i)
    outputTable_part = str(OutputTable)+str(tableNamePrefix)+str(i)

    #create tables table1_join_part_i , table2_join_part_i , outputtable1_join_part_i
    createNewTable(openconnection, table1_part, InputTable1)
    createNewTable(openconnection, table2_part, InputTable2)
    createNewTable(openconnection, outputTable_part, OutputTable)

    if i==0:
        #insert rows in table1_part where [min <= Table1JoinColumn <= max]
        insertQuery_table1_part = "INSERT INTO {0} SELECT * FROM {1} WHERE {2} >= {3} AND {2} <= {4};".format(table1_part, InputTable1, Table1JoinColumn, minValPart, maxValPart)
        cur.execute(insertQuery_table1_part)
        #insert rows in table2_part where [min <= Table2JoinColumn <= max]
        insertQuery_table2_part = "INSERT INTO {0} SELECT * FROM {1} WHERE {2} >= {3} AND {2} <= {4};".format(table2_part, InputTable2, Table2JoinColumn, minValPart, maxValPart)
        cur.execute(insertQuery_table2_part)
    
    else:
        #insert rows in table1_part where [min < Table1JoinColumn <=max]
        insertQuery_table1_part = "INSERT INTO {0} SELECT * FROM {1} WHERE {2} > {3} AND {2} <= {4};".format(table1_part, InputTable1, Table1JoinColumn, minValPart, maxValPart)
        cur.execute(insertQuery_table1_part)
        #insert rows in table2_part where [min < Table1JoinColumn <=max]
        insertQuery_table2_part = "INSERT INTO {0} SELECT * FROM {1} WHERE {2} > {3} AND {2} <= {4};".format(table2_part, InputTable2, Table2JoinColumn, minValPart, maxValPart)
        cur.execute(insertQuery_table2_part)
    
    #perform inner join on table1_part.Table1JoinColumn and table2_part.Table2JoinColumn
    joinQuery = "INSERT INTO {0} SELECT * FROM {1} INNER JOIN {2} ON {1}.{3} = {2}.{4};".format(outputTable_part, table1_part, table2_part, Table1JoinColumn, Table2JoinColumn)
    cur.execute(joinQuery)

    conn.commit()

#function to get MIN value in colName in tableName
def getMin(openconnection, colName, tablename):
    conn = openconnection
    cur = conn.cursor()

    minQuery = "SELECT MIN({0}) FROM {1};".format(colName, tablename)
    cur.execute(minQuery)
    return cur.fetchone()[0]

#function to get MAX value in colName in tableName
def getMax(openconnection, colName, tablename):
    conn = openconnection
    cur = conn.cursor()

    maxQuery = "SELECT MAX({0}) FROM {1};".format(colName, tablename)
    cur.execute(maxQuery)
    return cur.fetchone()[0]

#function returns schema of tableName
def getSchema(openconnection, tableName):
    conn = openconnection
    cur = conn.cursor()

    query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{0}'".format(tableName)
    cur.execute(query)

    return cur.fetchall()

#drop table if tableName exists. Create tableName LIKE likeTable
def createNewTable(openconnection, tableName, likeTable):
    conn = openconnection	
    cur = conn.cursor()
    
    dropQuery = "DROP TABLE IF EXISTS {0};".format(tableName)
    cur.execute(dropQuery)

    createQuery = "CREATE TABLE {0} (LIKE {1} INCLUDING ALL);".format(tableName, likeTable)
    cur.execute(createQuery)

    conn.commit()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


