"""Unit tests for ingestion layer"""
import io
import tempfile
from pathlib import Path

import pytest
from openpyxl import Workbook

from src.ingestion.csv_parser import CSVParser
from src.ingestion.excel_parser import ExcelParser
from src.ingestion.service import IngestionService
from src.ingestion.exceptions import UnsupportedFileTypeError


class TestExcelParser:
    """Tests for Excel parser"""
    
    def test_excel_parser_with_formulas(self, sample_excel_file):
        """Test parsing Excel file with formulas"""
        parser = ExcelParser()
        result = parser.parse(sample_excel_file)
        
        assert result.data is not None
        assert result.data.height >= 2
        assert "Metric" in result.data.columns
        assert "Value" in result.data.columns
        
        # Verify data extracted
        assert result.metadata["row_count"] >= 2
        assert result.metadata["filename"].endswith(".xlsx")
    
    def test_excel_parser_merged_cells(self, sample_excel_merged):
        """Test parsing Excel with merged cells"""
        parser = ExcelParser()
        result = parser.parse(sample_excel_merged)
        
        assert result.data is not None
        assert result.data.height >= 1
        
        # Verify merged cells were handled
        assert result.metadata["row_count"] >= 1
    
    def test_excel_parser_data_region_detection(self, sample_excel_file):
        """Test data region detection"""
        parser = ExcelParser()
        result = parser.parse(sample_excel_file)
        
        # Should detect headers and data
        assert len(result.data.columns) >= 2
        assert result.data.height >= 1
    
    def test_excel_parser_multiple_sheets(self):
        """Test parsing Excel with multiple sheets"""
        wb = Workbook()
        
        # Sheet 1
        ws1 = wb.active
        ws1.title = "Sheet1"
        ws1['A1'] = "Header1"
        ws1['A2'] = "Data1"
        
        # Sheet 2
        ws2 = wb.create_sheet("Sheet2")
        ws2['A1'] = "Header2"
        ws2['A2'] = "Data2"
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            wb.save(tmp.name)
            tmp_path = tmp.name
        
        try:
            parser = ExcelParser()
            result = parser.parse(tmp_path)
            
            # Should have data from both sheets
            assert result.data is not None
            assert "_sheet_name" in result.data.columns
            assert len(result.metadata["sheets"]) == 2
        finally:
            Path(tmp_path).unlink(missing_ok=True)


class TestCSVParser:
    """Tests for CSV parser"""
    
    def test_csv_parser_delimiter_detection_comma(self, sample_csv_comma):
        """Test comma delimiter detection"""
        parser = CSVParser()
        result = parser.parse(sample_csv_comma)
        
        assert result.metadata["delimiter"] == ","
        assert result.data.height == 2
        assert "Name" in result.data.columns
    
    def test_csv_parser_delimiter_detection_semicolon(self, sample_csv_semicolon):
        """Test semicolon delimiter detection"""
        parser = CSVParser()
        result = parser.parse(sample_csv_semicolon)
        
        assert result.metadata["delimiter"] == ";"
        assert result.data.height == 2
    
    def test_csv_parser_delimiter_detection_tab(self, sample_csv_tab):
        """Test tab delimiter detection"""
        parser = CSVParser()
        result = parser.parse(sample_csv_tab)
        
        assert result.metadata["delimiter"] == "\t"
        assert result.data.height == 2
    
    def test_csv_parser_encoding_detection(self, sample_csv_comma):
        """Test encoding detection"""
        parser = CSVParser()
        encoding = parser.detect_encoding(sample_csv_comma)
        
        assert encoding in ['utf-8', 'UTF-8', 'ascii', 'ASCII']
    
    def test_csv_parser_clean_data(self):
        """Test data cleaning"""
        import polars as pl
        
        parser = CSVParser()
        df = pl.DataFrame({
            "Name": ["  Alice  ", " Bob "],
            "Value": ["123", "456"]
        })
        
        cleaned = parser.clean_data(df)
        
        # Check whitespace stripped
        assert cleaned["Name"][0] == "Alice"
        assert cleaned["Name"][1] == "Bob"


class TestIngestionService:
    """Tests for ingestion service"""
    
    def test_ingestion_service_get_parser_excel(self, test_db):
        """Test parser factory for Excel"""
        service = IngestionService(test_db)
        parser = service.get_parser("xlsx")
        
        assert isinstance(parser, ExcelParser)
    
    def test_ingestion_service_get_parser_csv(self, test_db):
        """Test parser factory for CSV"""
        service = IngestionService(test_db)
        parser = service.get_parser("csv")
        
        assert isinstance(parser, CSVParser)
    
    def test_ingestion_service_unsupported_type(self, test_db):
        """Test unsupported file type raises error"""
        service = IngestionService(test_db)
        
        with pytest.raises(UnsupportedFileTypeError):
            service.get_parser("txt")
    
    def test_ingestion_service_database_save(self, test_db, sample_csv_comma):
        """Test file ingestion saves to database"""
        service = IngestionService(test_db)
        result = service.ingest_file(sample_csv_comma, "csv")
        
        assert result.upload_id is not None
        assert result.filename.endswith(".csv")
        assert result.row_count == 2
        assert result.column_count >= 3
        assert len(result.headers) >= 3
        
        # Verify database record
        upload = service.get_upload_status(result.upload_id)
        assert upload is not None
        assert upload.filename == result.filename
    
    def test_ingestion_service_extract_metadata(self, test_db):
        """Test metadata extraction"""
        import polars as pl
        
        service = IngestionService(test_db)
        df = pl.DataFrame({
            "A": [1, 2, None],
            "B": ["x", "y", "z"]
        })
        
        metadata = service.extract_metadata(df)
        
        assert metadata["row_count"] == 3
        assert metadata["column_count"] == 2
        assert "A" in metadata["column_names"]
        assert "missing_percentages" in metadata
        assert metadata["missing_percentages"]["A"] > 0


class TestIngestionAPI:
    """Tests for ingestion API endpoints"""

    def test_api_upload_endpoint(self, client, sample_csv_comma):
        """Test file upload endpoint"""
        with open(sample_csv_comma, 'rb') as f:
            response = client.post(
                "/api/v1/ingest/upload",
                files={"file": ("test.csv", f, "text/csv")},
                data={
                    "facility_name": "Test Facility",
                    "reporting_period": "2024-01"
                }
            )

        assert response.status_code == 201
        data = response.json()
        assert "upload_id" in data
        assert data["filename"] == "test.csv"
        assert data["status"] == "completed"

    def test_api_invalid_file_type(self, client):
        """Test upload with invalid file type"""
        file_content = b"This is a text file"

        response = client.post(
            "/api/v1/ingest/upload",
            files={"file": ("test.txt", io.BytesIO(file_content), "text/plain")},
            data={
                "facility_name": "Test Facility",
                "reporting_period": "2024-01"
            }
        )

        assert response.status_code == 400
        assert "Invalid file type" in response.json()["detail"]

    def test_api_get_upload_details(self, client, sample_csv_comma):
        """Test consolidated GET /{upload_id} returns status + preview"""
        with open(sample_csv_comma, 'rb') as f:
            upload_response = client.post(
                "/api/v1/ingest/upload",
                files={"file": ("test.csv", f, "text/csv")},
                data={
                    "facility_name": "Test Facility",
                    "reporting_period": "2024-01"
                }
            )

        upload_id = upload_response.json()["upload_id"]

        response = client.get(f"/api/v1/ingest/{upload_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["upload_id"] == upload_id
        assert data["status"] == "completed"
        assert data["filename"] == "test.csv"
        assert "metadata" in data
        assert data["metadata"]["row_count"] >= 1
        assert "headers" in data
        assert isinstance(data["headers"], list)
        assert "preview" in data
        assert isinstance(data["preview"], list)
        assert "errors" in data

    def test_api_get_upload_details_not_found(self, client):
        """Test GET /{upload_id} returns 404 for missing upload"""
        import uuid
        fake_id = str(uuid.uuid4())
        response = client.get(f"/api/v1/ingest/{fake_id}")
        assert response.status_code == 404

    def test_api_delete_upload(self, client, sample_csv_comma):
        """Test delete upload endpoint"""
        with open(sample_csv_comma, 'rb') as f:
            upload_response = client.post(
                "/api/v1/ingest/upload",
                files={"file": ("test.csv", f, "text/csv")},
                data={
                    "facility_name": "Test Facility",
                    "reporting_period": "2024-01"
                }
            )

        upload_id = upload_response.json()["upload_id"]

        response = client.delete(f"/api/v1/ingest/{upload_id}")

        assert response.status_code == 200
        assert "deleted successfully" in response.json()["message"]
