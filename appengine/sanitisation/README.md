# Extra details for sanitisation script

The current uploaded data has a format that looks like
```
id,date,fsa,probable,vulnerable,q1,q2,q3,q4,q5,q6,q7,q8
2826d3d9b20c7c0d0ffa7d694e5ed59636246a005a3602fba539c29a62db2e85,2020-04-02,M4P,y,y,y,y,y,y,y,y,y,y
8dc7796284b572418979d048e3c40f0015f7a4928b441cc3bc62550b06601903,2020-04-02,A2B,y,y,n,n,n,n,n,n,n,n
...
```

The timestamp of the uploaded script is before the first `-` in the filename. This is in milliseconds since the unix origin.

In terms of the schema:
* ID is a SHA256 hash of the ID in the datastore.
* `probable` is a flag (`y` or `n`) indicating whether the user's response indicates that they possibly have COVID-19.
* `vulnerable` is a flag (`y` or `n`) indicating whether the user's response indicates that they are elderly or immunocompromised.
* fsa is forward sortation area - first 3 digits of postcode. Note responses corrosponding to FSAs with <50 people in census data have been removed.
* Timestamp is in the form `YYYY-MM-DD`
* Question responses may be `y`, `n`, or nonexistent (though q1 through 7 should exist for all of the data)

Questions used to generate form responses:
* `q1`: Do you have a fever, chills or shakes?
* `q2`: Do you have a new or worsening cough?
* `q3`: Are you experiencing shortness of breath (difficulty breathing, breathlessness)?
* `q4`: Are you 60 years of age or older?
* `q5`: Do you have any of the following medical conditions: diabetes, heart disease, active cancer, history of stroke, asthma, COPD, dialysis, or are immunocompromised?
* `q6`: Have you traveled outside of Canada within the last 14 days?
* `q7`: Have you had close contact with someone who is coughing, has a fever, or is otherwise sick and has been outside of Canada in the last 14 days or has been diagnosed with COVID-19?
* `q8`: Have you been diagnosed with COVID-19?
* `fsa`: What are the first three characters of the postal code of your current residence?

Algorithm to determine `vulnerable`:
`q4 OR q5`

Algorithm to determine `probable`:
`q3 OR (q1 AND (q2 OR q6)) OR (q6 AND (q2 OR q3)) OR q7`
