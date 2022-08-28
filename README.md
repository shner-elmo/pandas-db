# db wrapper

### A wrapper for SQLite3

To instantiate the connection: 
```python
from src import DataBase
db = DataBase(db_path='data/forestation.db')
```

Get all tables:
```python
print(db.tables)
```
```
['forest_area', 'land_area', 'regions']
```

Access a table:
```py
table = db.forest_area
print(f'{table.columns=}, {table.shape=}')
```
```
table.columns=['country_code', 'country_name', 'year', 'forest_area_sqkm'], table.shape=(5886, 4)
```

Get first three rows:
```py
data = db.forest_area.data(3)
print(data)
```
```
[('ABW', 'Aruba', 2016, 4.199999869),
 ('AFG', 'Afghanistan', 2016, 13500.0),
 ('AGO', 'Angola', 2016, 577311.9922)]
```

You can also access tables and columns with brackets (just like in Pandas):
```py
col = db['land_area']['country_name']
print(f'{col.type=}, {col.len=}')
```
```
col.type='TEXT', col.len=5886
```
Get table rows:
```python
for row in db.land_area:
    print(row)
```
```
('ABW', 'Aruba', 2016, 69.5)
('AFG', 'Afghanistan', 2016, 252069.5)
('AGO', 'Angola', 2016, 481351.35)
('ALB', 'Albania', 2016, 10579.15)
...
```

Or with columns:
```py
for name in db.land_area.country_name:
    print(name)
```
```
Aruba
Afghanistan
Angola
Albania
...
```

To view the results of a query in a Dataframe:
```python
q = """
SELECT * FROM forest_area
JOIN regions
ON regions.country_code = forest_area.country_code
AND regions.country_name = forest_area.country_name
"""
db.query(q)
```
```
     country_code  ...         income_group
0             ABW  ...          High income
1             AFG  ...           Low income
2             AGO  ...  Lower middle income
3             ALB  ...  Upper middle income
4             AND  ...          High income
           ...  ...                  ...
5719          XKX  ...  Lower middle income
5720          YEM  ...           Low income
5721          ZAF  ...  Upper middle income
5722          ZMB  ...  Lower middle income
5723          ZWE  ...           Low income
[5724 rows x 8 columns]
```

And finally, when you're done don't forget to close the SQL connection:
```py
db.exit()
```

Using a context manager:
```python
with DataBase(db_path='data/forestation.db') as db:
    print(db.regions.region.data(5))
```
```
['South Asia', 
 'Europe & Central Asia', 
 'Middle East & North Africa',
 'East Asia & Pacific', 
 'Europe & Central Asia']
```

TODO:
- [ ] Finish Expression tests
- [ ] Complete Expression docstrings
- [ ] Add test for tables created after init
- [x] Add setup.py file
