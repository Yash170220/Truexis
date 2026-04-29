"""Custom exceptions for data ingestion"""


class ParseError(Exception):
    """Base exception for parsing errors"""
    pass


class AuthenticationError(ParseError):
    """Raised when file is password-protected"""
    pass


class FileFormatError(ParseError):
    """Raised when file format is invalid"""
    pass


class EmptyFileError(ParseError):
    """Raised when file is empty"""
    pass


class MalformedCSVError(ParseError):
    """Raised when CSV has inconsistent columns"""
    pass


class UnsupportedFileTypeError(ParseError):
    """Raised when file type is not supported"""
    pass
