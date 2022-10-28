
class FileTypeError(Exception):
    """ Raised when a given file type isn't valid """
    pass


class InvalidTableError(Exception):
    """ Raised when requested table isn't present in the DataBase """
    pass


class InvalidColumnError(Exception):
    """ Raised when requested column isn't present in the DataBase """
    pass


class ExpressionError(Exception):
    """ Raised when trying to use an instance of Expression with a non-Expression instance """
    pass


class ConnectionClosedWarning(Warning):
    """ Raised when using a closed SQL connection """
    pass
