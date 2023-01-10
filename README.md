# Pandas-DB

### A lightweight object for analyzing data directly from a Database without having to load anything onto memory

---
[![PyPi](https://img.shields.io/badge/PyPi-2.0.2-yellow)](https://pypi.org/project/pandasdb2/)
[![Downloads](https://pepy.tech/badge/pandasdb2)](https://pepy.tech/project/pandasdb2)
[![Downloads](https://pepy.tech/badge/pandasdb2/month)](https://pepy.tech/project/pandasdb2)

You can get the package directly from [PyPI](https://pypi.org/project/pandasdb2/)
```
pip install pandasdb2
```
---
## Why use Pandas-DB?

Pandas-db is a read-only package that allows you to view, analyze and explore all the content in a given Database (supported file extensions: db, sql, and sqlite)

The advantage of using this over something like Pandas is that your not storing anything onto memory and therefore the `db` object is very light compared to a regular Pandas `DataFrame` 

### How does it work?
The instance only stores the names of the tables as attributes (which is great for auto-complete), the Table and Column objects as the attribute values, and of course a reference to the SQLite connection object. 

So whenever you call a method to get some data, for ex: `db.table.data(10)` or `db.column.describe()`;  
the function runs a query that gets the data directly from the Database, so there is no need to store anything in memory.  

You can think of it as a wrapper for SQL so there is no need to type SQL queries manually, but also has some of the most common methods and functions from Pandas so you also don't need to store the table data onto memory. 

And to top it off, it makes it easy to import the tables from a database onto pandas for further analysis, for ex:
```python
from pandasdb2 import Database
db = Database(db_path='data/parch-and-posey.sql')
db.tables
```
```
['web_events', 'sales_reps', 'region', 'orders', 'accounts']
```

```
df1 = db.orders.to_df()
df2 = db.accounts.to_df()
type(df1), type(df2)
```
```
(pandas.core.frame.DataFrame, pandas.core.frame.DataFrame)
```


---
## Memory Usage

Now let's visualize this and see the amount of memory a `Database` object consumes compared to a Pandas `DataFrame`

Import the package:
```python
from pandasdb2 import Database
```
Create a connection:
```python
db = Database(db_path='.../yfin_data.db')
```

For example, this Database contains one table with more than 18 million rows:
```python
db.tables, db.stock_data.shape
```
```
(['stock_data'], (18078890, 9))
```


`db` object size:
```python
from pandasdb2.utils import get_gb_size
get_gb_size(db)
```
```
0.000005 GB
```

`dataframe` object size:
```python
dataframe = db.stock_data.to_df()
get_gb_size(dataframe)
```
```
6.961891 GB
```
As you can see, while the Database object remains almost the same size the Dataframe increases exponentially.

---

### Now let's dive onto the pandasdb package

Once you have the package installed, import the package:
```python
from pandasdb2 import Database
```

To instantiate the Database class you need to pass the path to the Database file, which could be one of the following extensions: db, sql, or sqlite
```python
db = Database(db_path='data/forestation.db')
```

### Tables
Get a list of all the tables 
```python
db.tables
```
```
['forest_area', 'land_area', 'regions']
```

To get the table object we can either use square brackets or type the name as an attribute, just like in Pandas
```python
db['forest_area']
```
```
     country_code  country_name  year  forest_area_sqkm
0             ABW         Aruba  2016           4.20000
1             AFG   Afghanistan  2016       13500.00000
2             AGO        Angola  2016      577311.99220
3             ALB       Albania  2016        7705.39978
4             AND       Andorra  2016         160.00000
           ...           ...   ...               ...
5881          XKX        Kosovo  1990               NaN
5882          YEM   Yemen, Rep.  1990        5490.00000
5883          ZAF  South Africa  1990       92410.00000
5884          ZMB        Zambia  1990      528000.00000
5885          ZWE      Zimbabwe  1990      221640.00000
[5886 rows x 4 columns]
```

Or:
```python
db.forest_area
```
```
     country_code  country_name  year  forest_area_sqkm
0             ABW         Aruba  2016           4.20000
1             AFG   Afghanistan  2016       13500.00000
2             AGO        Angola  2016      577311.99220
3             ALB       Albania  2016        7705.39978
4             AND       Andorra  2016         160.00000
           ...           ...   ...               ...
5881          XKX        Kosovo  1990               NaN
5882          YEM   Yemen, Rep.  1990        5490.00000
5883          ZAF  South Africa  1990       92410.00000
5884          ZMB        Zambia  1990      528000.00000
5885          ZWE      Zimbabwe  1990      221640.00000
[5886 rows x 4 columns]
```

Get a list of the table columns
```python
db.forest_area.columns
```
```
['country_code', 'country_name', 'year', 'forest_area_sqkm']
```

Get table shape:
```python
db.forest_area.shape
```
```
(5886, 4)
```

Get the first 20 rows 
```python
db.forest_area.data(20)
```
```
[('ABW', 'Aruba', 2016, 4.199999869),
 ('AFG', 'Afghanistan', 2016, 13500.0),
 ('AGO', 'Angola', 2016, 577311.9922),
 ('ALB', 'Albania', 2016, 7705.39978),
 ('AND', 'Andorra', 2016, 160.0),
 ...
```
Or you can use the `iloc` property to get a slice:
```python
db.regions.iloc[5:10]
```
```
[('Angola', 'AGO', 'Sub-Saharan Africa', 'Lower middle income'),
 ('Antigua and Barbuda', 'ATG', 'Latin America & Caribbean', 'High income'),
 ('Argentina', 'ARG', 'Latin America & Caribbean', 'High income'),
 ('Armenia', 'ARM', 'Europe & Central Asia', 'Upper middle income'),
 ('Aruba', 'ABW', 'Latin America & Caribbean', 'High income')]
```
---

### Columns:
```python
db.regions.region
```
```
Out[18]: 
0                      South Asia
1           Europe & Central Asia
2      Middle East & North Africa
3             East Asia & Pacific
4           Europe & Central Asia
                  ...            
214    Middle East & North Africa
215    Middle East & North Africa
216            Sub-Saharan Africa
217            Sub-Saharan Africa
218                         World
Name: region, Length: 219, dtype: object
```

Get the Python and SQL data type of the column:
```python
db.regions.region.type, db.regions.region.sql_type
```
```
(str, 'TEXT')
```

Describe the column: 
```python
db.forest_area.country_name.describe()
```
```
{'len': 5886,
 'count': 5886,
 'min': 'Afghanistan',
 'max': 'Zimbabwe',
 'unique': 218}
```
Note that the len returns the number of rows/ items in the column, while count excludes None values

Describe a numeric column:
```python
db.forest_area.forest_area_sqkm.describe()
```
```
{'len': 5886,
 'count': 5570,
 'min': 0.799999982,
 'max': 41282694.9,
 'sum': 2178158753.6738772,
 'avg': 391051.84087502287,
 'median': 20513.00049}
```

Get a dictionary with each distinct value and its count:
```python
db.regions.region.value_counts()
```
```
{'Europe & Central Asia': 58,
 'Sub-Saharan Africa': 48,
 'Latin America & Caribbean': 42,
 'East Asia & Pacific': 38,
 'Middle East & North Africa': 21,
 'South Asia': 8,
 'North America': 3,
```

Get the most common value:
```python
db.regions.income_group.mode()
```
```
{'High income': 81}
```

Get all unique/ distinct values in a column:
```python
db.forest_area.country_name.unique()
```
```
['Aruba',
 'Afghanistan',
 'Angola',
 ...
 'Zambia',
 'Zimbabwe']
```

When you're done you should always call the `exit()` method to close the SQL connection safely
```python
db.exit()
```

Now let's use another Database
```python
db = Database(db_path='data/parch-and-posey.sql')

for table in db.tables:
    print(table, db.get_columns(table_name=table))
```
```
web_events ['id', 'account_id', 'occurred_at', 'channel']
sales_reps ['id', 'name', 'region_id']
region ['id', 'name']
orders ['id', 'account_id', 'occurred_at', 'standard_qty', 'gloss_qty', 'poster_qty', 'total', 'standard_amt_usd', 'gloss_amt_usd', 'poster_amt_usd', 'total_amt_usd']
accounts ['id', 'name', 'website', 'lat', 'long', 'primary_poc', 'sales_rep_id']
```

You can apply a function to the whole column like so: 
```python
column = db.accounts.primary_poc.apply(lambda x: x.split(' ')[-1])

for last_name in column:
    print(last_name)
```
```
Tuma
Shields
Lupo
Banda
Crusoe
...
```

Similarly, the Table object has an `applymap` method, which maps/ applies the function on each cell in the table

First, let's have a look at the table:
```python
db.sales_reps
```
```
        id                 name  region_id
0   321500        Samuel Racine          1
1   321510         Eugena Esser          1
2   321520      Michel Averette          1
3   321530        Renetta Carew          1
4   321540          Cara Clarke          1
..     ...                  ...        ...
45  321950         Elwood Shutt          4
46  321960  Maryanna Fiorentino          4
47  321970  Georgianna Chisholm          4
48  321980       Micha Woodford          4
49  321990          Dawna Agnew          4

[50 rows x 3 columns]
```

And now we're going to pass a lambda function that will take the cell value and return the number of characters by converting it to a string:
```python
table = db.sales_reps.applymap(lambda x: len(str(x)))

for row in table:
    print(row)
```
```
(6, 13, 1)
(6, 12, 1)
(6, 15, 1)
(6, 13, 1)
(6, 11, 1)
...
```

You can also iterate directly on the table/ column object: 
```python
for row in db.sales_reps:
    print(row)
```
```
(321500, 'Samuel Racine', 1)
(321510, 'Eugena Esser', 1)
(321520, 'Michel Averette', 1)
(321530, 'Renetta Carew', 1)
(321540, 'Cara Clarke', 1)
...
```

Convert a table to a `DataFrame`:
```python
df = db.orders.to_df()
type(df)
```
```
pandas.core.frame.DataFrame
```

Similarly, you can convert a column to a Pandas `Series`
```python
ser = db.orders.occurred_at.to_series()
type(ser)
```
```
pandas.core.series.Series
```



### Filtering

Just like Pandas you can filter a given table by passing a column object with an operator, eg: `col >= 32`
```python
db = Database('data/parch-and-posey.sql')
db.tables
```
```
['web_events', 'sales_reps', 'region', 'orders', 'accounts']
```

Lets save the table that we'll be working with as df
```python
df = db.orders
```
```
        id  account_id          occurred_at  standard_qty  gloss_qty  poster_qty  total  standard_amt_usd  gloss_amt_usd  poster_amt_usd  total_amt_usd
0        1        1001  2015-10-06 17:31:14           123         22          24    169            613.77         164.78          194.88         973.43
1        2        1001  2015-11-05 03:34:33           190         41          57    288            948.10         307.09          462.84        1718.03
2        3        1001  2015-12-04 04:21:55            85         47           0    132            424.15         352.03            0.00         776.18
3        4        1001  2016-01-02 01:18:24           144         32           0    176            718.56         239.68            0.00         958.24
4        5        1001  2016-02-01 19:27:27           108         29          28    165            538.92         217.21          227.36         983.49
    ...         ...                  ...           ...        ...         ...    ...               ...            ...             ...            ...
6907  6908        4501  2016-06-29 04:03:39            11        199          59    269             54.89        1490.51          479.08        2024.48
6908  6909        4501  2016-07-29 19:58:32             5         91          96    192             24.95         681.59          779.52        1486.06
6909  6910        4501  2016-08-27 00:58:11            16         94          82    192             79.84         704.06          665.84        1449.74
6910  6911        4501  2016-11-22 06:52:22            63         67          81    211            314.37         501.83          657.72        1473.92
6911  6912        4501  2016-12-21 13:30:42            61        150          52    263            304.39        1123.50          422.24        1850.13

[6912 rows x 11 columns]
```

Get all the orders where the account-id is equal to 4091
```python
df[df.account_id == 4091]
```
```
     id  account_id          occurred_at  standard_qty  gloss_qty  poster_qty  total  standard_amt_usd  gloss_amt_usd  poster_amt_usd  total_amt_usd
0  3775        4091  2016-11-22 07:33:05           325         36          46    407           1621.75         269.64          373.52        2264.91
1  3776        4091  2016-12-21 09:22:20           300         28           0    328           1497.00         209.72            0.00        1706.72
2  6573        4091  2016-11-22 07:57:27            45        482         305    832            224.55        3610.18         2476.60        6311.33

[3 rows x 11 columns]
```

Get all the orders where the total quantity is between 300 - 500
```python
df[df.total.between(300, 500)]
```
```
        id  account_id          occurred_at  standard_qty  gloss_qty  poster_qty  total  standard_amt_usd  gloss_amt_usd  poster_amt_usd  total_amt_usd
0       25        1041  2016-10-14 23:54:21           298         28          69    395           1487.02         209.72          560.28        2257.02
1       26        1041  2016-11-13 10:11:52           307         22           0    329           1531.93         164.78            0.00        1696.71
2       30        1051  2016-10-01 00:48:28           486          0           1    487           2425.14           0.00            8.12        2433.26
3       33        1051  2016-12-30 08:45:43           495          1           1    497           2470.05           7.49            8.12        2485.66
4       34        1061  2016-10-19 16:04:11           290         52          23    365           1447.10         389.48          186.76        2023.34
    ...         ...                  ...           ...        ...         ...    ...               ...            ...             ...            ...
1693  6596        4121  2016-11-20 13:47:32           172         63         219    454            858.28         471.87         1778.28        3108.43
1694  6634        4161  2014-06-17 14:02:42            33        305          40    378            164.67        2284.45          324.80        2773.92
1695  6663        4181  2016-06-24 07:40:53           426         59           0    485           2125.74         441.91            0.00        2567.65
1696  6855        4451  2014-03-09 07:21:16           241         27          42    310           1202.59         202.23          341.04        1745.86
1697  6868        4461  2014-08-25 04:07:47            22        185         291    498            109.78        1385.65         2362.92        3858.35

[1698 rows x 11 columns]
```

Pass multiple conditions:
```python
df[(df.total >= 500) & (df.total_amt_usd > 6700)]
```
```
       id  account_id          occurred_at  standard_qty  gloss_qty  poster_qty  total  standard_amt_usd  gloss_amt_usd  poster_amt_usd  total_amt_usd
0      24        1031  2016-12-25 03:54:27          1148          0         215   1363           5728.52           0.00         1745.80        7474.32
1     129        1141  2016-12-21 15:52:58           143       1045        2157   3345            713.57        7827.05        17514.84       26055.46
2     176        1181  2016-04-24 16:47:51          2188         50          12   2250          10918.12         374.50           97.44       11390.06
3     214        1221  2016-02-04 20:17:54           485       1345          21   1851           2420.15       10074.05          170.52       12664.72
4     234        1231  2016-11-20 15:16:58           505          0        1355   1860           2519.95           0.00        11002.60       13522.55
..    ...         ...                  ...           ...        ...         ...    ...               ...            ...             ...            ...
774  6885        4461  2016-02-04 21:12:41            52        581         276    909            259.48        4351.69         2241.12        6852.29
775  6890        4461  2016-09-26 08:44:11            42        538         313    893            209.58        4029.62         2541.56        6780.76
776  6899        4491  2014-03-06 05:22:25           549        523         245   1317           2739.51        3917.27         1989.40        8646.18
777  6903        4491  2014-09-28 15:53:06            52        601         360   1013            259.48        4501.49         2923.20        7684.17
778  6906        4491  2015-01-24 07:15:47            54        621         282    957            269.46        4651.29         2289.84        7210.59

[779 rows x 11 columns]
```

Just like in Pandas you can also save the filtered dataframe/table to a variable
and perform operations on it 
```python
filtered_df = df[(df.total >= 500) & (df.total_amt_usd > 4700)]

filtered_df.shape
```
```
(1491, 11)
```

```python
filtered_df.account_id.value_counts()
```
```
{3411: 33,
 2591: 30,
 4211: 30,
 1561: 29,
 2181: 29,
 4151: 29,
 1401: 28,
 ...}
```

Sort a table:
```python
filtered_df.sort_values('total_amt_usd', ascending=False)
```
```
        id  account_id          occurred_at  standard_qty  gloss_qty  poster_qty  total  standard_amt_usd  gloss_amt_usd  poster_amt_usd  total_amt_usd
0     4016        4251  2016-12-26 08:53:24           521         16       28262  28799           2599.79         119.84       229487.44      232207.07
1     3892        4161  2016-06-24 13:32:55         22591         13           6  22610         112729.09          97.37           48.72      112875.18
2     3963        4211  2015-03-30 00:05:30           114      14281           0  14395            568.86      106964.69            0.00      107533.55
3     5791        2861  2014-10-24 12:06:22             0         10       11691  11701              0.00          74.90        94930.92       95005.82
4     3778        4101  2016-07-17 14:50:43           475          3       11226  11704           2370.25          22.47        91155.12       93547.84
    ...         ...                  ...           ...        ...         ...    ...               ...            ...             ...            ...
1486  4721        1491  2015-06-13 21:50:46            46        426         160    632            229.54        3190.74         1299.20        4719.48
1487  6528        3991  2016-11-17 06:56:25             0         63         523    586              0.00         471.87         4246.76        4718.63
1488  4719        1491  2015-04-15 14:19:14            54        485         100    639            269.46        3632.65          812.00        4714.11
1489  5214        2081  2014-06-06 16:55:16            44        600           0    644            219.56        4494.00            0.00        4713.56
1490  4081        4291  2015-08-09 17:47:52           302        416          11    729           1506.98        3115.84           89.32        4712.14

[1491 rows x 11 columns]
```

You can also sort by multiple columns with either a `list` or `dict`'
```python
filtered_df.sort_values({'account_id': 'asc', 'total_amt_usd': 'desc'})
```
```
        id  account_id          occurred_at  standard_qty  gloss_qty  poster_qty  total  standard_amt_usd  gloss_amt_usd  poster_amt_usd  total_amt_usd
0     4308        1001  2015-12-04 04:01:09           526        597         287   1410           2624.74        4471.53         2330.44        9426.71
1     4309        1001  2016-01-02 00:59:09           566        645         194   1405           2824.34        4831.05         1575.28        9230.67
2     4316        1001  2016-08-28 06:50:58           557        572         255   1384           2779.43        4284.28         2070.60        9134.31
3     4317        1001  2016-09-26 23:22:47           507        614         226   1347           2529.93        4598.86         1835.12        8963.91
4     4314        1001  2016-05-31 21:09:48           531        603         209   1343           2649.69        4516.47         1697.08        8863.24
    ...         ...                  ...           ...        ...         ...    ...               ...            ...             ...            ...
1486  6900        4491  2014-05-05 00:03:19            33        508         283    824            164.67        3804.92         2297.96        6267.55
1487  6905        4491  2014-12-26 21:39:04           218        366         283    867           1087.82        2741.34         2297.96        6127.12
1488  6896        4491  2013-12-08 06:34:23            43        520         242    805            214.57        3894.80         1965.04        6074.41
1489  6901        4491  2014-07-31 05:05:06            12        509         262    783             59.88        3812.41         2127.44        5999.73
1490  6898        4491  2014-02-04 03:04:08            34        517         205    756            169.66        3872.33         1664.60        5706.59

[1491 rows x 11 columns]
```

```python
sorted_df = filtered_df.sort_values({'account_id': 'asc', 'total_amt_usd': 'desc'})
for row in sorted_df:
    print(row)
```
```
(4308, 1001, '2015-12-04 04:01:09', 526, 597, 287, 1410, 2624.74, 4471.53, 2330.44, 9426.71)
(4309, 1001, '2016-01-02 00:59:09', 566, 645, 194, 1405, 2824.34, 4831.05, 1575.28, 9230.67)
(4316, 1001, '2016-08-28 06:50:58', 557, 572, 255, 1384, 2779.43, 4284.28, 2070.6, 9134.31)
(4317, 1001, '2016-09-26 23:22:47', 507, 614, 226, 1347, 2529.93, 4598.86, 1835.12, 8963.91)
(4314, 1001, '2016-05-31 21:09:48', 531, 603, 209, 1343, 2649.69, 4516.47, 1697.08, 8863.24)
(4307, 1001, '2015-11-05 03:25:21', 506, 612, 203, 1321, 2524.94, 4583.88, 1648.36, 8757.18)
(4311, 1001, '2016-03-02 15:40:29', 498, 605, 204, 1307, 2485.02, 4531.45, 1656.48, 8672.95)
...
```
---

## Cache

When initializing the `Database()` object there are a few parameters which will 
determine how the output is cached (if it is at all).


* `cache` (True/False, default True)

By default, all the SQL queries are cached, 
so whenever you do `db.table.col.median()` it will calculate the median
and cache the result for next time, this is done for almost all `Table` and `Column` 
properties (where the output is always the same or not very large).

* `populate_cache` (True/False, default False)

If the parameter is set to True it will loop through all the tables and columns 
in the database and populate the cache, so whenever you do `table.shape` or 
`col.avg()` the result will already be there.
Note that populating the cache can take quite some time for larger databases.

* `max_item_size`(int, default 2), and `max_dict_size` (int, default 100)

The two parameters determine the maximum size in Megabytes for the element to be cached.
For example: if the output of `col.value_counts()` was 2.1MB and max_item_size is set to 2MB,
then the output would not be cached since it goes over the limit.

And if the output was 1.9MB and the current cache size was 99MB then it would not
cache the output of the function because otherwise the whole cache-dict size would
be above 100MB.

[//]: # (One caveat is that when the output of the query is very large, for example:)

[//]: # (if you do `db.table.col.value_counts&#40;&#41;` and the column values are unique, then)

[//]: # (`len&#40;output&#41;` will be equal to `len&#40;col&#41;`)

---
And finally, you can pass an SQL query to `db.query()` which will return a Pandas `DataFrame` with the results:
```python
query = """
SELECT * FROM accounts
JOIN sales_reps
    ON sales_reps.id = accounts.sales_rep_id
JOIN region
    ON region.id = sales_reps.region_id
"""
db.query(query)
```
```
           long  ...    id_2               name_2  region_id id_3     name_3  
0    -75.103297  ...  321500        Samuel Racine          1    1  Northeast  
1    -73.849374  ...  321510         Eugena Esser          1    1  Northeast  
2    -76.084009  ...  321520      Michel Averette          1    1  Northeast  
3    -75.763898  ...  321530        Renetta Carew          1    1  Northeast  
4    -75.284998  ...  321540          Cara Clarke          1    1  Northeast  
..          ...  ...     ...                  ...        ...  ...        ...  
346 -122.655247  ...  321970  Georgianna Chisholm          4    4       West  
347 -122.681500  ...  321960  Maryanna Fiorentino          4    4       West  
348 -122.669460  ...  321970  Georgianna Chisholm          4    4       West  
349 -122.671880  ...  321960  Maryanna Fiorentino          4    4       West  
350 -122.657145  ...  321970  Georgianna Chisholm          4    4       West  

[351 rows x 12 columns]
```

Close the connection
```python
db.exit()
```

 ---

TODO:
 - [x] Move Table and Column objects to a dictionary
 - [x] Support mathematical operations between Column objects (db.table.col1 * db.table.col2)
 - [x] Replace `to_string()` in `__repr__()` with a custom one
 - [x] Add filter function
 - [x] Add cache system for most common properties
 - [ ] Add Group-By method
