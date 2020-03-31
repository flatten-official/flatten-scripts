# Extra details for sanitisation script

The current uploaded data has a format that looks like
```
postal_code,timestamp,q1,q2,q3,q4,q5,q6,q7
A2B,1585620321951,n,n,n,n,n,n,n
V1N,1585622396472,n,y,n,y,n,y,n
A2B,1585668584665,n,n,n,n,n,n,n
```

The timestamp of the uploaded stript is before the first `-` in the filename. This is in milliseconds since the unix origin.

