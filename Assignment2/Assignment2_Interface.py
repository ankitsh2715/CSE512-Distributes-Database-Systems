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

    #get min value for SortingColumnName
    minQuery = "SELECT MIN({0}) FROM {1};".format(SortingColumnName, InputTable)
    cur.execute(minQuery)
    minVal = cur.fetchone()[0]

    #get max value for SortingColumnName
    maxQuery = "SELECT MAX({0}) FROM {1};".format(SortingColumnName, InputTable)
    cur.execute(maxQuery)
    maxVal = cur.fetchone()[0]

    numThreads = 5          # 5 threads to be used
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
    
    #drop any previous output table that may exist from other test cases
    dropOutputTableQuery = "DROP TABLE IF EXISTS {0};".format(OutputTable)
    cur.execute(dropOutputTableQuery)

    #create output table with same schema as input table
    createOutputTableQuery = "CREATE TABLE {0} (LIKE {1} INCLUDING ALL);".format(OutputTable, InputTable)
    cur.execute(createOutputTableQuery)

    for i in range(numThreads):
        #insert rows in output table in order partTable0, partTable1, partTable2....
        insertSortPartitionQuery = "INSERT INTO {0} SELECT * FROM {1}{2};".format(OutputTable, tableNamePrefix, i)
        cur.execute(insertSortPartitionQuery)
    
    for i in range(numThreads):
        #delete temporary part tables that we created
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









def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    


    pass # Remove this once you are done with implementation





















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


