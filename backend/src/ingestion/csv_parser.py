"""CSV file parser with auto-detection capabilities"""
import logging
from pathlib import Path
from typing import Dict, Any

import chardet
import polars as pl

from src.ingestion.exceptions import EmptyFileError, MalformedCSVError, ParseError
from src.ingestion.base_parser import BaseParser, ParsedDataFrame

logger = logging.getLogger(__name__)


class CSVParser(BaseParser):
    """Parser for CSV files with intelligent detection"""

    def parse(self, file_path: str) -> ParsedDataFrame:
        """Parse CSV file and return DataFrame with metadata"""
        logger.info(f"Parsing CSV file: {file_path}")
        
        # Check if file is empty
        if Path(file_path).stat().st_size == 0:
            raise EmptyFileError("CSV file is empty")
        
        # Detect encoding
        encoding = self.detect_encoding(file_path)
        logger.info(f"Detected encoding: {encoding}")
        
        # Read sample for delimiter detection
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                sample = ''.join([f.readline() for _ in range(5)])
        except Exception as e:
            raise ParseError(f"Failed to read file: {str(e)}")
        
        if not sample.strip():
            raise EmptyFileError("CSV file contains no data")
        
        # Detect delimiter
        delimiter = self.detect_delimiter(sample)
        logger.info(f"Detected delimiter: {repr(delimiter)}")
        
        # Parse CSV with polars
        try:
            df = pl.read_csv(
                file_path,
                separator=delimiter,
                encoding=encoding,
                ignore_errors=False,
                infer_schema_length=1000,
                null_values=["", "NULL", "null", "NA", "N/A", "n/a"]
            )
        except pl.exceptions.ComputeError as e:
            if "could not parse" in str(e).lower():
                raise MalformedCSVError(f"Inconsistent column count in CSV: {str(e)}")
            raise ParseError(f"Failed to parse CSV: {str(e)}")
        except Exception as e:
            # Try alternative encodings
            for alt_encoding in ['utf-8', 'iso-8859-1', 'windows-1252']:
                if alt_encoding == encoding:
                    continue
                try:
                    logger.info(f"Retrying with encoding: {alt_encoding}")
                    df = pl.read_csv(
                        file_path,
                        separator=delimiter,
                        encoding=alt_encoding,
                        ignore_errors=False,
                        infer_schema_length=1000
                    )
                    encoding = alt_encoding
                    break
                except Exception:
                    continue
            else:
                raise ParseError(f"Failed to parse CSV with any encoding: {str(e)}")
        
        # Clean data
        df = self.clean_data(df)
        
        if df.height == 0:
            raise EmptyFileError("CSV file contains no valid data rows")
        
        # Build metadata
        metadata = {
            "filename": Path(file_path).name,
            "encoding": encoding,
            "delimiter": delimiter,
            "rows": df.height,
            "columns": df.width,
            "headers": df.columns,
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
        }
        
        logger.info(f"Successfully parsed {metadata['rows']} rows, {metadata['columns']} columns")
        
        return ParsedDataFrame(data=df, metadata=metadata)

    def detect_delimiter(self, sample: str) -> str:
        """Detect the most likely delimiter from sample text"""
        delimiters = {
            ',': 0,
            ';': 0,
            '\t': 0,
            '|': 0
        }
        
        lines = sample.strip().split('\n')
        if not lines:
            return ','
        
        # Count occurrences in each line
        for line in lines[:5]:
            for delim in delimiters:
                delimiters[delim] += line.count(delim)
        
        # Find delimiter with highest count
        max_delim = max(delimiters, key=delimiters.get)
        
        # If no delimiter found, default to comma
        if delimiters[max_delim] == 0:
            logger.warning("No delimiter detected, defaulting to comma")
            return ','
        
        return max_delim

    def detect_encoding(self, file_path: str) -> str:
        """Detect file encoding using chardet"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB
            
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            
            logger.info(f"Encoding detection: {encoding} (confidence: {confidence:.2f})")
            
            # If confidence is low, default to UTF-8
            if confidence < 0.7:
                logger.warning(f"Low confidence ({confidence:.2f}), defaulting to UTF-8")
                return 'utf-8'
            
            return encoding if encoding else 'utf-8'
        
        except Exception as e:
            logger.warning(f"Encoding detection failed: {e}, defaulting to UTF-8")
            return 'utf-8'

    def clean_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean and normalize DataFrame"""
        logger.info("Cleaning data")
        
        # Remove rows where all values are null
        df = df.filter(~pl.all_horizontal(pl.all().is_null()))
        
        # Strip whitespace from string columns
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col(col).str.strip_chars().alias(col)
                )
        
        # Convert numeric-looking strings to numbers
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                try:
                    # Try to cast to float
                    numeric_col = df[col].str.replace_all(',', '').cast(pl.Float64, strict=False)
                    # If more than 50% are valid numbers, convert
                    valid_count = numeric_col.is_not_null().sum()
                    if valid_count > len(df) * 0.5:
                        df = df.with_columns(numeric_col.alias(col))
                        logger.info(f"Converted column '{col}' to numeric")
                except Exception:
                    pass
        
        logger.info(f"Cleaned data: {df.height} rows remaining")
        return df
