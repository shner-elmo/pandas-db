from __future__ import annotations

from typing import Literal

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
        self.table = table  # TODO remove self.table attribute?

    def __and__(self, expression: Expression) -> Expression:
        """
        Return an Expression object with 'self.query' as: 'Expression1 AND Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        :raise: ExpressionError if self.table != other.table
        """
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        if not self.table == expression.table:
            raise ExpressionError(
                f'Cannot concatenate two expressions from different tables ({self.table} and {expression.table})')

        return Expression(query=f'{self.query} AND {expression.query}', table=self.table)

    def __or__(self, expression: Expression) -> Expression:
        """
        Return an Expression object with 'self.query' as: 'Expression1 OR Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        :raise: ExpressionError if self.table != other.table
        """
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        if not self.table == expression.table:
            raise ExpressionError(
                f'Cannot concatenate two expressions from different tables ({self.table} and {expression.table})')

        return Expression(query=f'{self.query} OR {expression.query}', table=self.table)

    def __str__(self) -> str:
        """ Get string representation of Expression instance """
        return f'SELECT ... WHERE {self.query}'

    def __repr__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(query={repr(self.query)}, table={repr(self.table)})'


class OrderBy:
    def __init__(self, column: str | list[str] | dict[str, Literal['ASC', 'DESC']], ascending: bool = True) -> None:
        """
        An object that represents an SQL ORDER-BY statement

        You can pass:
        1) A column-name with the optional parameter 'ascending'
        2) A list of column names, which will all be ordered ascending
        3) A dictionary with the column-name and the sorting order, for ex:
        {'col1': 'ASC', 'col2': 'DESC', 'col3': 'ASC'}

        :param column: str | list | dict, column or list of columns
        :param ascending: bool, default True
        """
        if isinstance(column, str):
            self.cols = f'{column} {"ASC" if ascending else "DESC"}'

        elif isinstance(column, list):
            self.cols = f'{", ".join(column)}'

        elif isinstance(column, dict):
            cols = [f'{col} {sort_order}' for col, sort_order in column.items()]
            self.cols = f'{", ".join(cols)}'

        else:
            raise TypeError(f'column parameter must be str, list, or dict, not: {type(column)}')

        # save args for repr()
        self._column = column
        self._ascending = ascending

    def __str__(self) -> str:
        """ Get string representation of Expression instance """
        return f'SELECT ... ORDER BY {self.cols}'

    def __repr__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(column={repr(self._column)}, ascending={self._ascending})'


class Limit:
    def __init__(self, limit: int) -> None:
        """
        An object that represents an SQL LIMIT statement

        :param limit: int
        """
        self.limit = limit

    def __str__(self) -> str:
        """ Get string representation of Expression instance """
        return f'SELECT ... LIMIT {self.limit}'

    def __repr__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(limit={self.limit})'
