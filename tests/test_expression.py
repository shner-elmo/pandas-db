import unittest

from pandasdb.expression import Expression
from pandasdb.exceptions import ExpressionError


class TestExpression(unittest.TestCase):
    def test_init(self):
        self.assertRaises(
            TypeError,
            Expression, query=['a', 'b', 'c'], table='region'
        )

    def test_validate_other(self):
        a = Expression(query='price BETWEEN 3.23 AND 49.0', table='products')
        b = 'id IN (3209, 2093, 1905)'
        self.assertRaisesRegex(
            ExpressionError,
            'expression must be an instance of Expression, try using a column object instead',
            lambda: a & b
        )

        a = Expression(query='price BETWEEN 3.23 AND 49.0', table='products')
        b = Expression(query='id IN (3209, 2093, 1905)', table='students')
        self.assertRaisesRegex(
            ExpressionError,
            'Cannot concatenate two expressions from different tables',
            lambda: a & b
        )

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
