# multiLevelMapReduce
This is Java based implementation for a multilevel mapreduce program (conversion, sorting &amp; filtering)

Input :
A big-data file from a e-commerce website with content given as "SessionId,Timestamp,ItemId"

Usecase :
Calculate the top-N items which were clicked in the month of April

Output :
Part file with output given as "ItemId  ClickCount"

Implementation :
The first map-reduce operation converts the input file into an output of type "ItemId, Count". Now to calculate the Top-N items
which have the maximum count, we add another map-reduce operation, with a sortComparator in between to sort the output coming
from the mapper to the reducer.
Note : The reducer & combinator for the second map-reduce have been optimized to only forward the top-N items (as they arrive 
there in a Descending order)
