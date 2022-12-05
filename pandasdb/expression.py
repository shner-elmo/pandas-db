from __future__ import annotations

from .exceptions import ExpressionError


class Expression:
    """
    A class for converting Python's logical operators to SQL-compatible strings
    """
    def __init__(self, query: str, table: str) -> None:
        """
        An object that represents an SQL expression/filter ("price BETWEEN 5 AND 15")

        :param query: str, e.g., id = 3571
        :param table: str, table-name
        """
        if not isinstance(query, str):
            raise TypeError(f'parameter query must be of type str, not: {type(query)}')

        self.query = query
        self.table = table

    def _validate_other(self, other) -> None:
        """
        Validate other expression is valid (for __and__() and __or__())

        :raise: ExpressionError if param: expression isn't an instance of Expression
        :raise: ExpressionError if self.table != other.table
        """
        if not isinstance(other, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        if not self.table == other.table:
            raise ExpressionError(
                f'Cannot concatenate two expressions from different tables ({self.table} and {other.table})')

    def __and__(self, expression: Expression) -> Expression:
        """
        Return an Expression object with 'self.query' as: 'Expression1 AND Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        :raise: ExpressionError if self.table != other.table
        """
        self._validate_other(other=expression)
        return Expression(query=f'{self.query} AND {expression.query}', table=self.table)

    def __or__(self, expression: Expression) -> Expression:
        """
        Return an Expression object with 'self.query' as: 'Expression1 OR Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        :raise: ExpressionError if self.table != other.table
        """
        self._validate_other(other=expression)
        return Expression(query=f'{self.query} OR {expression.query}', table=self.table)

    def __str__(self) -> str:
        """ Get string representation of Expression instance """
        return f'SELECT ... WHERE {self.query}'

    def __repr__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(query={repr(self.query)}, table={repr(self.table)})'
