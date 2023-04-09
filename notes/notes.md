# Notes

## Challenge Two

To reduce latency, we write out data from S3 to local storage as parquet files in the `data` directory. We only have to write the files out once, this reduces the necessity to make queries over a network.

We test the threshold for joining by iterating over a set of thresholds for joins (of longitide and lattitude columns). The thresholds are applied symmetrically. A report of this test, shown below, is used as a guide to guess the best value for the threshold for joins (0.0045 degrees). We collect the following metrics 
* Coverage: a measure of missed joins.
* Similarity: comparison of the reference data against the query produced by us.
* Missed joins: the number of missing joins.

The looser the threshold for joins, the more the duplicates will be observed after joins. We filter duplicated rows out by selecting the row group with the smallest delta from the geojson data. Getting a threshold error down to zero has drawbacks. We find that as coverage improves (fewer failed joins) we observe an increase in similarity scores (measurement of errors against the reference data) up until similarity peaks at 89.8%, increasing the join threshold from this point degrades the quality of the data. The best result produces a coverage of 99.7% and a similarity score of 88.8% (missing 2700 joins). The script will error out when coverage is below 85%.

Table 1: Metrics testing the quality of joining service data to geojson data.

 | similarity | threshold | Coverage | Missed Joins |
 |------------|-----------|----------|--------------|
 |  0.270283  |   0.0010  | 0.270283 |       687126 |
 |  0.327796  |   0.0015  | 0.327796 |       632970 |
 |  0.406829  |   0.0020  | 0.406829 |       558550 |
 |  0.508833  |   0.0025  | 0.508833 |       462500 |
 |  0.635164  |   0.0030  | 0.635164 |       343542 |
 |  0.779067  |   0.0035  | 0.784639 |       202791 |
 |  0.898336  |   0.0040  | 0.944905 |        51879 |
 |  0.888184  |   0.0045  | 0.997133 |         2700 |
 |  0.822788  |   0.0050  | 0.999999 |            1 |
 |  0.741667  |   0.0055  | 1.000000 |            0 |


## Challenge Three

Used 'sr_hex' data to get all H8 index codes for suburb 'BELLVILLE SOUTH'. Eight codes were found; these were then used to filter `city-hex-polygons-8.geojson` to retrieve
the centroids of each. We then used the arithmetic mean to find the longitude and latititude centroid coordinates respectively. To find all notifications within 1 minute we converted decimal degrees to degrees, minutes, seconds and filtered for locations within one minute of the centroid.

We have also anonymised data by adding random degrees to latitude and longitude data, where the perturbations would be limited to around 500m.
The exact location has to be anonymised in order to limit leaks of personal identifiable information (PII). It is important to keep the identities of citizens
who log calls private.
