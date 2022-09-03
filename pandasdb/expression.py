from .exceptions import ExpressionError


class Expression:
    """
    A class for converting Python's logical operators to SQL-compatible strings
    """
    def __init__(self, query: str) -> None:
        self.query = query

    def __and__(self, expression: 'Expression') -> 'Expression':
        """
        Return an Expression object with 'self.query' as: 'Expression1 AND Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        """
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        return Expression(query=f'{self.query} AND {expression.query} ')

    def __or__(self, expression: 'Expression') -> 'Expression':
        """
        Return an Expression object with 'self.query' as: 'Expression1 OR Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        """
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        return Expression(query=f'{self.query} OR {expression.query} ')

    def __str__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(query="{self.query}")'

    def __repr__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(query="{self.query}")'
