import unittest

from pandasdb.expression import Expression, OrderBy, Limit


class TestExpression(unittest.TestCase):
    def test_and(self):
        a = Expression(query='name == "jake"', table='accounts')
        b = Expression(query='age >= 24', table='accounts')

        a_and_b = a & b
        self.assertEqual(a_and_b.query, 'name == "jake" AND age >= 24')

        c = Expression(query='city_code IN ("LA", "NY", "LV")', table='accounts')
        abc = a_and_b & c
        query = 'name == "jake" AND age >= 24 AND city_code IN ("LA", "NY", "LV")'
        self.assertEqual(abc.query, query)

    def test_or(self):
        a = Expression(query='name == "jake"', table='accounts')
        b = Expression(query='age >= 24', table='accounts')

        a_or_b = a | b
        self.assertEqual(a_or_b.query, 'name == "jake" OR age >= 24')

        c = Expression(query='city_code IN ("LA", "NY", "LV")', table='accounts')
        three_expressions = a_or_b | c
        query = 'name == "jake" OR age >= 24 OR city_code IN ("LA", "NY", "LV")'
        self.assertEqual(three_expressions.query, query)

    def test_str(self):
        a = Expression(query='name == "jake"', table='accounts')
        self.assertIn(member=a.query, container=str(a))

    def test_repr(self):
        a = Expression(query='name == "jake"', table='accounts')
        eval(repr(a))


class TestOrderBy(unittest.TestCase):
    def test_init(self):
        # with column as str:
        x = OrderBy(column='income')
        self.assertTrue(hasattr(x, 'query'))
        self.assertIsInstance(x.query, str)
        self.assertEqual(x.query, 'ORDER BY income ASC')

        x = OrderBy(column='income', ascending=True)
        self.assertEqual(x.query, 'ORDER BY income ASC')

        x = OrderBy(column='income', ascending=False)
        self.assertEqual(x.query, 'ORDER BY income DESC')

        # with column as list:
        x = OrderBy(column=['name', 'age', 'city'])
        self.assertEqual(x.query, 'ORDER BY name, age, city')

        # with column as dict:
        x = OrderBy(column={'name': 'ASC', 'age': 'DESC', 'city': 'ASC'})
        self.assertEqual(x.query, 'ORDER BY name ASC, age DESC, city ASC')

    def test_str(self):
        x = OrderBy(column='income', ascending=False)
        self.assertIn(member=x.query, container=str(x))

        x = OrderBy(column=['name', 'age', 'city'])
        self.assertIn(member=x.query, container=str(x))

        x = OrderBy(column={'name': 'ASC', 'age': 'DESC', 'city': 'ASC'})
        self.assertIn(member=x.query, container=str(x))

    def test_repr(self):
        # test repr is reproducible
        x = OrderBy(column='income', ascending=True)
        eval(repr(x))

        x = OrderBy(column=['name', 'age', 'city'])
        eval(repr(x))

        x = OrderBy(column={'col1': 'ASC', 'col2': 'DESC', 'col3': 'ASC'})
        eval(repr(x))


class TestLimit(unittest.TestCase):
    def test_init(self):
        x = Limit(limit=20)
        self.assertTrue(hasattr(x, 'query'))
        self.assertEqual(x.query, 'LIMIT 20')

        # note that Sqlite3 allows passing both a zero or and a negative value,
        # if zero is passed as the limit, then it returns zero rows,
        # but if the value is less than zero, than it will return all rows
        x = Limit(limit=0)
        self.assertTrue(hasattr(x, 'query'))
        self.assertEqual(x.query, 'LIMIT 0')

        x = Limit(limit=-34)
        self.assertTrue(hasattr(x, 'query'))
        self.assertEqual(x.query, 'LIMIT -34')

    def test_str(self):
        x = Limit(limit=20)
        self.assertIn(member=x.query, container=str(x))

    def test_repr(self):
        x = Limit(limit=20)
        self.assertIn(member=x.query, container=str(x))


if __name__ == '__main__':
    unittest.main()
