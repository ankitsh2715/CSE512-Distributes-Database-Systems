Map Class :-

-> Get joinColumn value from row using split() function with "," as separator
-> Add key-value pair to output where key is joinColumn and value is entire row

Reduce Class :-

-> Input is provided in the form (joinColumn, <row_x, row_y, row_z......>)
-> Iterate over all values for joinColumn and place rows in two different sets based on tablename. Set1 for one table and Set2 for another table. We only need two sets because it is given that only two different table name will be in the input.
-> Run two nested for-loops iterating over Set1 and Set2 and concatenate rows in both sets which provides all required joins.
-> Set key=rowJoin and value=null and add it to output.

Driver(main) func :-

-> Get reference to a JobConf object
-> Set data type of key-value output, name of mapper and reducer class and input output paths passed via command line arguments.
-> Finally call runJob() function with this configuration.