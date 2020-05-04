

Format of the form data that is uploaded:
```
{
    "region": {
        # each district with data for the last 24h and a time series
        "district_name": {
        "pot": 23,
        "risk": 29,
        "number_individuals": 38
        "number_reports": 58
        },
      ... blah
    },
    # time series of each day of data
    "region_time_series": {
       "all_reports": { # contains all of the reports for all regions aggregated
            "2020-04-28": {"pot": 17, "risk": 189, "reports": 259, "deaths": 120}
        },
       "district_name": {
            "2020-04-28": {"pot": 17, "risk": 189, "reports": 259, "deaths": 120}
        }
        
    }
    
}
```