# Equijoin-Map-Reduce
The required task is to write a map-reduce program that will perform equijoin.

The code is in Java (Java 1.8.x) using Hadoop Framework (use Hadoop 2.7.x).

The code would take two inputs, one would be the HDFS location of the file on which the equijoin should be  performed and other would be the HDFS location of the file, where the output should be stored.

Format of the Input File: -  
Table1Name, Table1JoinColumn, Table1Other Column1, Table1OtherColumn2, ……..  
Table2Name, Table2JoinColumn, Table2Other Column1, Table2OtherColumn2, ……...  

Format of the Output File: -  
If Table1JoinColumn value is equal to Table2JoinColumn value, simply append both line side by side for Output. If Table1JoinColumn value does not match any value of Table2JoinColumn, simply remove them for the output file. You should not include duplicate joins in output file.

Note: -  
Table1JoinColumn and Table2JoinColumn would both be Integer or Real or Double or Float, basically numeric.  

## Example Input : -  
R, 2, Don, Larson, Newark, 555-3221  
S, 1, 33000, 10000, part1  
S, 2, 18000, 2000, part1  
S, 2, 20000, 1800, part1  
R, 3, Sal, Maglite, Nutley, 555-6905  
S, 3, 24000, 5000, part1  
S, 4, 22000, 7000, part1  
R, 4, Bob, Turley, Passaic, 555-8908  

## Example Output: -  
R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1  
R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1  
R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1  
S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908  

Another correct answer is:  
R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1  
R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1  
R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1  
R, 4, Bob, Turley, Passaic, 555-8908, S, 4, 22000, 7000, part1  

So it means that whether R is before S is not required in the result.  
But you cannot have both  

S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908  
and  
R, 4, Bob, Turley, Passaic, 555-8908, S, 4, 22000, 7000, part1  
  
in the output.  

You cannot assume that the table are R and S all the time. They can be other two tables. Number of
tables in the input are exactly 2.  

## Running the code:  

sudo -u <username> <path_of_hadoop> jar <name_of_jar> <class_with_main_function> <HDFSinputFile> <HDFSoutputFile>

### Example: -  
sudo -u hduser /usr/local/hadoop/bin/hadoop jar equijoin.jar equijoin hdfs://localhost:54310/input/sample.txt hdfs://localhost:54310/output

## Code Logic:  
    
The code consists of 3 parts:     
	a) Driver method  
	b) Mapper class which implements the map function  
	c) Reducer class which implements the reduce function  
	  
### Driver Method:  

•	The driver takes all the components that we’ve built for our MapReduce job and pieces them together to be submitted for execution.  
•	The driver orchestrates the job.  
•	It first parses the command line input to read the input and output path.  
•	It also sets up a job object by telling it that EquiJoinMapper is the mapper class, EquiJoinReducer is the reducer class.  
•	It also specifies the classes of keys and values given as output by Mapper and the Reducer.  
•	It sets up the input and output paths.  
•	Then it just starts the job.  


### Mapper class:  

•	It implements the map function.  
•	The map-reduce framework is designed in such a way that mapper and reducer process one key-value pair at a time.  
•	So, during the map phase, the key (which is generally the position of the record in input) is ignored and the value is parsed to fetch the join key column.  
•	The output of the map is a key-value pair where key is the join column and value is the complete record again (the value which we received as input to the mapper).  
•	This way, we can group records belonging to both relations based on the join key.  

### Reducer class:  
•	The input to the reducer class's reduce function is key-list of values, where key is the join column and list of values is an iterable consisting of all records with the same key from both relations.  
•	Since reducer also processes one (key-iterable values) at a time, we are sure that the records of both relations have the same key and can be combined.  
•	In reducer, we iterate over the list of values for each key and create 2 separate lists for both the relations. Now each record of one relation is concatenated with each record of second relation.   
•	If the key has corresponding records belonging to just one table, those records are ignored.  
•	The output of the reduce function is just the values created by concatenation in the step above.  
•	As per the expected output format, I am not outputting the keys as a part of output.  
