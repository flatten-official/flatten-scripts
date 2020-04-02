# Extra details for sanitisation script

The current uploaded data has a format that looks like
```
id,date,fsa,q1,q2,q3,q4,q5,q6,q7,q8
9d0d78b712693a4d44afdffd3de3cb356c5a1a745b68fe82afeedfcda77b10d2,2020-04-01,M4P,n,n,n,n,n,n,n,n
93ab06a1d5b291df9e05c26ebaca478da2cd48e109ffaa5c45967c00b1fe0584,2020-04-01,M4P,n,n,n,n,n,n,n,n
...
```

The timestamp of the uploaded script is before the first `-` in the filename. This is in milliseconds since the unix origin.

In terms of the schema:
* ID is a SHA256 hash of the ID in the datastore.
* fsa is forward sortation area - first 3 digits of postcode. Note responses corrosponding to FSAs with <50 people in census data have been removed.
* Timestamp is in the form `YYYY-MM-DD`
* Question responses may be `y`, `n`, or nonexistent (though q1 through 7 should exist for all of the data)
