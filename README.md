[//]: # (# Pandas-DB)

[//]: # ()
[//]: # (### A lightweight object for analyzing data directly from a Database without having to load anything onto memory)

[//]: # ()
[//]: # (You can now install this package directly from [pypi]&#40;https://pypi.org/project/pandasdb2/&#41;)

[//]: # (```)

[//]: # (pip install pandasdb2)

[//]: # (```)

[//]: # ()
[//]: # (To instantiate the connection: )

[//]: # (```python)

[//]: # (from pandasdb import DataBase)

[//]: # (db = DataBase&#40;db_path='data/parch-and-posey.sql'&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Get all tables:)

[//]: # (```python)

[//]: # (print&#40;db.tables&#41;)

[//]: # (```)

[//]: # (```)

[//]: # (['forest_area', 'land_area', 'regions'])

[//]: # (```)

[//]: # ()
[//]: # (Access a table:)

[//]: # (```py)

[//]: # (table = db.forest_area)

[//]: # (print&#40;f'{table.columns=}, {table.shape=}'&#41;)

[//]: # (```)

[//]: # (```)

[//]: # (table.columns=['country_code', 'country_name', 'year', 'forest_area_sqkm'], table.shape=&#40;5886, 4&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Get first three rows:)

[//]: # (```py)

[//]: # (data = db.forest_area.data&#40;3&#41;)

[//]: # (print&#40;data&#41;)

[//]: # (```)

[//]: # (```)

[//]: # ([&#40;'ABW', 'Aruba', 2016, 4.199999869&#41;,)

[//]: # ( &#40;'AFG', 'Afghanistan', 2016, 13500.0&#41;,)

[//]: # ( &#40;'AGO', 'Angola', 2016, 577311.9922&#41;])

[//]: # (```)

[//]: # ()
[//]: # (You can also access tables and columns with brackets &#40;just like in Pandas&#41;:)

[//]: # (```py)

[//]: # (col = db['land_area']['country_name'])

[//]: # (print&#40;f'{col.type=}, {col.len=}'&#41;)

[//]: # (```)

[//]: # (```)

[//]: # (col.type='TEXT', col.len=5886)

[//]: # (```)

[//]: # (Get table rows:)

[//]: # (```python)

[//]: # (for row in db.land_area:)

[//]: # (    print&#40;row&#41;)

[//]: # (```)

[//]: # (```)

[//]: # (&#40;'ABW', 'Aruba', 2016, 69.5&#41;)

[//]: # (&#40;'AFG', 'Afghanistan', 2016, 252069.5&#41;)

[//]: # (&#40;'AGO', 'Angola', 2016, 481351.35&#41;)

[//]: # (&#40;'ALB', 'Albania', 2016, 10579.15&#41;)

[//]: # (...)

[//]: # (```)

[//]: # ()
[//]: # (Or with columns:)

[//]: # (```py)

[//]: # (for name in db.land_area.country_name:)

[//]: # (    print&#40;name&#41;)

[//]: # (```)

[//]: # (```)

[//]: # (Aruba)

[//]: # (Afghanistan)

[//]: # (Angola)

[//]: # (Albania)

[//]: # (...)

[//]: # (```)

[//]: # ()
[//]: # (To view the results of a query in a Dataframe:)

[//]: # (```python)

[//]: # (q = """)

[//]: # (SELECT * FROM forest_area)

[//]: # (JOIN regions)

[//]: # (ON regions.country_code = forest_area.country_code)

[//]: # (AND regions.country_name = forest_area.country_name)

[//]: # (""")

[//]: # (db.query&#40;q&#41;)

[//]: # (```)

[//]: # (```)

[//]: # (     country_code  ...         income_group)

[//]: # (0             ABW  ...          High income)

[//]: # (1             AFG  ...           Low income)

[//]: # (2             AGO  ...  Lower middle income)

[//]: # (3             ALB  ...  Upper middle income)

[//]: # (4             AND  ...          High income)

[//]: # (           ...  ...                  ...)

[//]: # (5719          XKX  ...  Lower middle income)

[//]: # (5720          YEM  ...           Low income)

[//]: # (5721          ZAF  ...  Upper middle income)

[//]: # (5722          ZMB  ...  Lower middle income)

[//]: # (5723          ZWE  ...           Low income)

[//]: # ([5724 rows x 8 columns])

[//]: # (```)

[//]: # ()
[//]: # (And finally, when you're done don't forget to close the SQL connection:)

[//]: # (```py)

[//]: # (db.exit&#40;&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Using a context manager:)

[//]: # (```python)

[//]: # (with DataBase&#40;db_path='data/forestation.db'&#41; as db:)

[//]: # (    print&#40;db.regions.region.data&#40;5&#41;&#41;)

[//]: # (```)

[//]: # (```)

[//]: # (['South Asia', )

[//]: # ( 'Europe & Central Asia', )

[//]: # ( 'Middle East & North Africa',)

[//]: # ( 'East Asia & Pacific', )

[//]: # ( 'Europe & Central Asia'])

[//]: # (```)

[//]: # ()

[//]: # (- [ ] Finish Expression tests)

[//]: # (- [ ] Complete Expression docstrings)

[//]: # ()
[//]: # (# make sure all methods are present in tests)

[//]: # (# test apply, and applymap)

[//]: # (# rewrite all docs)