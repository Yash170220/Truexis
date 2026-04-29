"""Unit tests for CSV parser"""
import tempfile
from pathlib import Path

import polars as pl
import pytest

from src.ingestion.csv_parser import CSVParser
from src.ingestion.exceptions import EmptyFileError, MalformedCSVError


@pytest.fixture
def csv_parser():
    """Create CSV parser instance"""
    return CSVParser()


@pytest.fixture
def temp_csv_file():
    """Create temporary CSV file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        yield f.name
    Path(f.name).unlink(missing_ok=True)


def test_parse_simple_csv(csv_parser, temp_csv_file):
    """Test parsing a simple CSV file"""
    # Create test CSV
    with open(temp_csv_file, 'w') as f:
        f.write("Name,Age,City\n")
        f.write("Alice,30,NYC\n")
        f.write("Bob,25,LA\n")
    
    result = csv_parser.parse(temp_csv_file)
    
    assert result.data.height == 2
    assert result.data.width == 3
    assert result.metadata['delimiter'] == ','
    assert 'Name' in result.data.columns


def test_detect_delimiter_comma(csv_parser):
    """Test comma delimiter detection"""
    sample = "Name,Age,City\nAlice,30,NYC\n"
    delimiter = csv_parser.detect_delimiter(sample)
    assert delimiter == ','


def test_detect_delimiter_semicolon(csv_parser):
    """Test semicolon delimiter detection"""
    sample = "Name;Age;City\nAlice;30;NYC\n"
    delimiter = csv_parser.detect_delimiter(sample)
    assert delimiter == ';'


def test_detect_delimiter_tab(csv_parser):
    """Test tab delimiter detection"""
    sample = "Name\tAge\tCity\nAlice\t30\tNYC\n"
    delimiter = csv_parser.detect_delimiter(sample)
    assert delimiter == '\t'


def test_parse_empty_file(csv_parser, temp_csv_file):
    """Test parsing empty file raises error"""
    with open(temp_csv_file, 'w') as f:
        f.write("")
    
    with pytest.raises(EmptyFileError):
        csv_parser.parse(temp_csv_file)


def test_clean_data_removes_empty_rows(csv_parser):
    """Test that clean_data removes rows with all nulls"""
    df = pl.DataFrame({
        "A": [1, None, 3],
        "B": [2, None, 4]
    })
    
    cleaned = csv_parser.clean_data(df)
    assert cleaned.height == 2


def test_clean_data_strips_whitespace(csv_parser):
    """Test that clean_data strips whitespace from strings"""
    df = pl.DataFrame({
        "Name": ["  Alice  ", " Bob "],
        "City": [" NYC ", "  LA  "]
    })
    
    cleaned = csv_parser.clean_data(df)
    assert cleaned["Name"][0] == "Alice"
    assert cleaned["City"][1] == "LA"


def test_parse_with_different_encodings(csv_parser, temp_csv_file):
    """Test parsing files with different encodings"""
    # Create UTF-8 file
    with open(temp_csv_file, 'w', encoding='utf-8') as f:
        f.write("Name,Value\n")
        f.write("Test,123\n")
    
    result = csv_parser.parse(temp_csv_file)
    assert result.data.height == 1


def test_detect_encoding(csv_parser, temp_csv_file):
    """Test encoding detection"""
    with open(temp_csv_file, 'w', encoding='utf-8') as f:
        f.write("Name,Value\n")
    
    encoding = csv_parser.detect_encoding(temp_csv_file)
    assert encoding in ['utf-8', 'UTF-8', 'ascii', 'ASCII']


def test_parse_with_null_values(csv_parser, temp_csv_file):
    """Test parsing CSV with various null representations"""
    with open(temp_csv_file, 'w') as f:
        f.write("Name,Age,City\n")
        f.write("Alice,30,NYC\n")
        f.write("Bob,NA,\n")
        f.write("Charlie,null,LA\n")
    
    result = csv_parser.parse(temp_csv_file)
    assert result.data.height == 3
    assert result.data["Age"][1] is None or result.data["Age"][1] != result.data["Age"][1]  # Check for null/NaN


def test_clean_data_converts_numeric_strings(csv_parser):
    """Test that numeric strings are converted to numbers"""
    df = pl.DataFrame({
        "Value": ["123", "456", "789"]
    })
    
    cleaned = csv_parser.clean_data(df)
    # Check if column was converted to numeric type
    assert cleaned["Value"].dtype in [pl.Float64, pl.Int64]
