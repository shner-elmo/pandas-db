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

And finally, when you're done don't forget to close the SQL connection:
```py
db.exit()
```

TODO:
- [ ] Finish Expression tests
- [ ] Add test for tables created after init
- [x] Add setup.py file
