"""Base parser interface"""
from abc import ABC, abstractmethod
from typing import Dict, Any

import polars as pl


class ParsedDataFrame:
    """Container for parsed data with metadata"""
    def __init__(self, data: pl.DataFrame, metadata: Dict[str, Any]):
        self.data = data
        self.metadata = metadata


class BaseParser(ABC):
    """Abstract base class for file parsers"""
    
    @abstractmethod
    def parse(self, file_path: str) -> ParsedDataFrame:
        """Parse file and return DataFrame with metadata"""
        pass
