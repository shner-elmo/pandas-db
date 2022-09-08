# Pandas-DB

### A lightweight object for analyzing data directly from a Database without having to load anything onto memory

---

You can get the package directly from [PyPI](https://pypi.org/project/pandasdb2/)
```
pip install pandasdb2
```
---
## Why use Pandas-DB?

Pandas-db is a read-only package that allows you to view, analyze and explore all the content in a given Database (supported file extensions: db, sql, and sqlite)

The advantage of using this over something like Pandas is that your not storing anything onto memory and therefore the `db` object is very light compared to a regular Pandas `DataFrame` 

The instance only stores the names of the tables as attributes (which is great for auto-complete), the Table and Column objects as the attribute values, and of course a reference to the SQLite connection object. 

So whenever you call a method to get some data, for ex: `db.table.data(10)` or `db.column.describe()`;  
the function runs a query that gets the data directly from the Database, so there is no need to store anything in memory.  

You can think of it as a wrapper for SQLite so there is no need to type SQL queries manually, but also has some of the most common methods and functions from Pandas so you also don't need to store the table data onto memory. 

And to top it off, it makes it easy to import the tables from a database onto pandas for further analysis, for ex:
```python
from pandasdb import DataBase
db = DataBase(db_file)

df1 = db.orders.to_df()
df2 = db.accounts.to_df()
```


For the full tutorial click [here](#-Now-lets-dive-onto-the-pandasdb-library)

---
## Memory Usage

Now let's visualize this and see the amount of memory a `DataBase` object consumes compared to a Pandas `DataFrame`

Import the package:
```python
from pandasdb import DataBase
```
```python
db = DataBase('data/forestation.db')
```

Using `asizeof.asizeof()` from the `pympler` package we can get the number of Bytes an object is taking in memory
```python
from pympler import asizeof

def mb_size(*obj): 
    """ Print object size in Megabytes """
    size = asizeof.asizeof(*obj)
    print(f'{size / 1e+6:,f} MB')
```

Database object size in Megabytes:
```python
mb_size(db)
```
```
0.006320 MB
```
Less than 1% of a Megabyte, which is almost nothing

Now let's compare the memory it takes to store just one table as a DataFrame:
```python
df = db.forest_area.to_df()
mb_size(df)
```
```
1.691432 MB
```

And if we were to store all three tables from our Database as Dataframes, we will be using:
```python
tables = [db.forest_area.to_df(), db.land_area.to_df(), db.regions.to_df()]
mb_size(*tables)
```
```
3.510600 MB
```

Now 3.5 Megabytes isn't much, but you need to keep in mind that these tables only have about 5k rows each,
so as you start working with more data this number gets exponentially larger.

```python
# close database
db.exit()
```

For example, this Database contains one table with almost two million rows:
```python
db = DataBase('.../yfin_data.db')
print(f'tables={db.tables}')
print(f'shape={db.stock_data.shape}')
```
```
tables=['stock_data']
shape=(18078890, 9)
```

Function for getting the size in Gigabytes
```python
def gb_size(obj): 
    """ Print object size in Gigabytes """
    size = asizeof.asizeof(obj)
    print(f'{size / 1e+9:,f} GB')
```

`db` object size:
```python
gb_size(db)
```
```
0.000005 GB
```

`dataframe` object size:
```python
dataframe = db.stock_data.to_df()
gb_size(dataframe)
```
```
6.961891 GB
```
As you can see, while the Dataframe size increases the DataBase object remains almost the same size.

---

### Now let's dive onto the pandasdb package

Once you have the package installed, import the package:
```python
from pandasdb import DataBase
```

To instantiate the DataBase class you need to pass the path to the Database file, which could be one of the following extensions: db, sql, or sqlite
```python
db = DataBase('data/forestation.db')
```

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

Columns:
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
db = DataBase('data/parch-and-posey.sql')

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

And finally, you can pass an SQL query to `db.query()` which will return a `DataFrame` with the results:
```python
q = """
SELECT * FROM forest_area
JOIN regions
    ON regions.country_code = forest_area.country_code -- remove name and keep code ?#
    AND regions.country_name = forest_area.country_name
JOIN land_area
    ON land_area.country_code = forest_area.country_code
    AND land_area.country_name = forest_area.country_name
    AND land_area.year = forest_area.year
"""
df = db.query(q)
df
```
```
        id  account_id          occurred_at  ...    id_3               name_2 region_id
0        1        1001  2015-10-06 17:13:58  ...  321500        Samuel Racine         1
1        2        1001  2015-11-05 03:08:26  ...  321500        Samuel Racine         1
2        3        1001  2015-12-04 03:57:24  ...  321500        Samuel Racine         1
3        4        1001  2016-01-02 00:55:03  ...  321500        Samuel Racine         1
4        5        1001  2016-02-01 19:02:33  ...  321500        Samuel Racine         1
...    ...         ...                  ...  ...     ...                  ...       ...
9068  9069        4491  2016-10-04 15:43:29  ...  321960  Maryanna Fiorentino         4
9069  9070        4491  2016-10-04 23:42:41  ...  321960  Maryanna Fiorentino         4
9070  9071        4491  2016-11-06 07:23:45  ...  321960  Maryanna Fiorentino         4
9071  9072        4491  2016-12-18 03:21:31  ...  321960  Maryanna Fiorentino         4
9072  9073        4501  2016-05-30 00:46:53  ...  321970  Georgianna Chisholm         4

[9073 rows x 14 columns]
```

As you can see the duplicated columns are automatically renamed with a number at the end
```python
df.columns
```
```
Index(['id', 'account_id', 'occurred_at', 'channel', 'id_2', 'name', 'website',
       'lat', 'long', 'primary_poc', 'sales_rep_id', 'id_3', 'name_2',
       'region_id'],
      dtype='object')
```

Close the connection
```python
db.exit()
```

 ---

 TODO:
 - [ ] Move Table and Column objects to a dictionary
 - [ ] Support mathematical operations between Column objects (db.table.col1 * db.table.col2)
 - [ ] Replace `to_string()` in `__repr__()` with a custom one
 - [ ] Add filter function
 - [ ] Add cache system for most common properties
