# Formats

The current format page store only rows
no metadata so it is pretty sapce efficient. Insertion is also fast
because we just append to the end.

However, finding a particular row can only be done by scanning the entire table.
and if we want to delete a row we have to fill in the hole by moving every row
that comes after it.

If we stored the table as an array, 
but kept rows sorted by id, we could use binary search to find 
a particular id. However, insertion would be slow because we would 
have to move a lot of rows to make space.

Instead, we’re going with a tree structure. Each node in the
tree can contain a variable number of rows, so we have to store 
some information in each node to keep track of how many rows it 
contains. Plus there is the storage overhead of all the internal 
nodes which don’t store any rows. In exchange for a larger database
file, we get fast insertion, deletion and lookup.

Complexity

Unsorted Array of rows;

Pages contain: only data
Rows per page: more
Insertion: O(1)
Deletion: O(n)
Lookup: O(n)

Sorted Array of rows;
Pages contain: only data
Rows per page: more
Insertion: O(n)
Deletion: O(n)
Lookup: O(log n)

Tree of nodes:
Pages contain: metadata, primary keys, and data
Rows per page: less
Insertion: O(log n)
Deletion: O(log n)
Lookup: O(log n)



