"""Unified ingestion service for all file types"""
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from uuid import UUID

import polars as pl
from sqlalchemy.orm import Session

from src.common.models import Upload, UploadStatus, FileType, AuditLog, AuditAction
from src.common.provenance import get_provenance_tracker
from src.ingestion.base_parser import BaseParser
from src.ingestion.csv_parser import CSVParser
from src.ingestion.excel_parser import ExcelParser
from src.ingestion.exceptions import UnsupportedFileTypeError

logger = logging.getLogger(__name__)


class IngestionResult:
    """Result of file ingestion"""
    def __init__(
        self,
        upload_id: UUID,
        filename: str,
        row_count: int,
        column_count: int,
        headers: List[str],
        preview: Dict[str, List],
        errors: Optional[List[str]] = None
    ):
        self.upload_id = upload_id
        self.filename = filename
        self.row_count = row_count
        self.column_count = column_count
        self.headers = headers
        self.preview = preview
        self.errors = errors or []


class IngestionService:
    """Service for ingesting files into the system"""

    def __init__(self, db: Session):
        self.db = db

    def ingest_file(self, file_path: str, file_type: str) -> IngestionResult:
        """Ingest file and save to database"""
        logger.info(f"Ingesting file: {file_path} (type: {file_type})")
        
        upload_id = uuid.uuid4()
        filename = Path(file_path).name
        errors = []
        
        try:
            # Get appropriate parser
            parser = self.get_parser(file_type)
            
            # Parse file
            parsed = parser.parse(file_path)
            df = parsed.data
            metadata = parsed.metadata
            
            # Extract metadata
            extracted_metadata = self.extract_metadata(df)
            metadata.update(extracted_metadata)

            # Store preview in metadata (first 10 rows, column-oriented)
            preview_df = df.head(10)
            metadata["preview_data"] = {
                col: [str(v) if v is not None else None for v in preview_df[col].to_list()]
                for col in preview_df.columns
            }

            # Create upload record
            upload = Upload(
                id=upload_id,
                filename=filename,
                file_type=FileType(file_type.lower()),
                upload_time=datetime.utcnow(),
                status=UploadStatus.COMPLETED,
                file_path=file_path,
                file_metadata=metadata
            )
            self.db.add(upload)
            
            # Save data to database (staging)
            self.save_to_database(df, upload_id)
            
            # Log audit
            audit = AuditLog(
                entity_id=upload_id,
                entity_type="uploads",
                action=AuditAction.CREATED,
                actor="system",
                timestamp=datetime.utcnow(),
                changes={"filename": filename, "rows": df.height}
            )
            self.db.add(audit)
            
            self.db.commit()
            logger.info(f"Successfully ingested file: {upload_id}")

            # Record provenance
            end_time = datetime.utcnow()
            prov = get_provenance_tracker()
            activity_id = f"ingest_{upload_id}"
            prov.record_activity(
                activity_id, "file_ingestion",
                upload.upload_time, end_time, "system",
            )
            prov.record_entity(str(upload_id), "uploaded_file", {
                "filename": filename,
                "file_type": file_type,
                "rows": df.height,
                "columns": df.width,
            })
            prov.flush()

            # Generate preview (first 5 rows)
            preview = self._generate_preview(df)
            
            return IngestionResult(
                upload_id=upload_id,
                filename=filename,
                row_count=df.height,
                column_count=df.width,
                headers=df.columns,
                preview=preview,
                errors=errors
            )
            
        except Exception as e:
            logger.error(f"Failed to ingest file: {e}")
            
            # Create failed upload record
            upload = Upload(
                id=upload_id,
                filename=filename,
                file_type=FileType(file_type.lower()),
                upload_time=datetime.utcnow(),
                status=UploadStatus.FAILED,
                file_path=file_path,
                file_metadata={"error": str(e)}
            )
            self.db.add(upload)
            self.db.commit()
            
            errors.append(str(e))
            raise

    def ingest_file_from_upload(self, file, facility_name: str, reporting_period: str) -> IngestionResult:
        """Ingest file from FastAPI UploadFile"""
        import tempfile
        import shutil
        
        # Save uploaded file to temp location
        file_ext = Path(file.filename).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
        
        try:
            # Determine file type
            file_type = file_ext.lstrip('.')
            
            # Ingest the file
            result = self.ingest_file(tmp_path, file_type)
            
            # Move file to permanent storage
            upload_dir = Path("data/uploads") / str(result.upload_id)
            upload_dir.mkdir(parents=True, exist_ok=True)
            final_path = upload_dir / file.filename
            shutil.move(tmp_path, final_path)
            
            # Update file path in database
            upload = self.db.query(Upload).filter(Upload.id == result.upload_id).first()
            if upload:
                upload.file_path = str(final_path)
                upload.file_metadata["facility_name"] = facility_name
                upload.file_metadata["reporting_period"] = reporting_period
                self.db.commit()
            
            return result
        finally:
            # Clean up temp file if it still exists
            if Path(tmp_path).exists():
                Path(tmp_path).unlink()

    def get_parser(self, file_type: str) -> BaseParser:
        """Factory method to get appropriate parser"""
        file_type = file_type.lower()
        
        parsers = {
            "xlsx": ExcelParser,
            "xls": ExcelParser,
            "csv": CSVParser,
        }
        
        parser_class = parsers.get(file_type)
        if not parser_class:
            raise UnsupportedFileTypeError(
                f"File type '{file_type}' is not supported. "
                f"Supported types: {', '.join(parsers.keys())}"
            )
        
        return parser_class()

    def save_to_database(self, df: pl.DataFrame, upload_id: UUID) -> None:
        """Save DataFrame to database (staging area)"""
        logger.info(f"Saving {df.height} rows to database for upload {upload_id}")
        
        # Note: This is a simplified staging approach
        # In production, you might want to save to a staging table
        # or process the data through matching/normalization first
        
        # For now, we just log that data is ready for processing
        logger.info(f"Data staged for upload {upload_id}, ready for matching/normalization")

    def extract_metadata(self, df: pl.DataFrame) -> Dict:
        """Extract metadata from DataFrame"""
        metadata = {
            "row_count": df.height,
            "column_count": df.width,
            "column_names": df.columns,
            "data_types": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "missing_percentages": {}
        }
        
        # Calculate missing value percentages
        for col in df.columns:
            null_count = df[col].is_null().sum()
            percentage = (null_count / df.height * 100) if df.height > 0 else 0
            metadata["missing_percentages"][col] = round(percentage, 2)
        
        return metadata

    def _generate_preview(self, df: pl.DataFrame, rows: int = 5) -> Dict[str, List]:
        """Generate preview of first N rows"""
        preview_df = df.head(rows)
        return {col: preview_df[col].to_list() for col in preview_df.columns}

    def get_upload_status(self, upload_id: UUID) -> Optional[Upload]:
        """Get upload status by ID"""
        return self.db.query(Upload).filter(Upload.id == upload_id).first()

    def get_upload_details(self, upload_id: UUID) -> Optional[Dict]:
        """Get full upload details: status + metadata + preview combined"""
        upload = self.db.query(Upload).filter(Upload.id == upload_id).first()
        if not upload:
            return None

        meta = upload.file_metadata or {}

        headers = meta.get("column_names", [])
        preview_raw = meta.get("preview_data", {})

        # Convert column-oriented preview {col: [vals]} into row-oriented [{col: val}]
        preview_rows: List[Dict] = []
        if preview_raw and headers:
            num_rows = min(10, max((len(v) for v in preview_raw.values()), default=0))
            for i in range(num_rows):
                row = {}
                for col in headers:
                    vals = preview_raw.get(col, [])
                    row[col] = vals[i] if i < len(vals) else None
                preview_rows.append(row)

        file_size_mb = None
        if upload.file_path:
            try:
                file_size_mb = round(Path(upload.file_path).stat().st_size / (1024 * 1024), 2)
            except OSError:
                pass

        errors: List[str] = []
        if upload.status == UploadStatus.FAILED:
            err = meta.get("error")
            if err:
                errors.append(err)

        return {
            "upload_id": upload.id,
            "filename": upload.filename,
            "file_type": upload.file_type.value if upload.file_type else "unknown",
            "status": upload.status.value,
            "upload_time": upload.upload_time,
            "metadata": {
                "row_count": meta.get("row_count", 0),
                "column_count": meta.get("column_count", 0),
                "file_size_mb": file_size_mb,
                "facility_name": meta.get("facility_name"),
                "reporting_period": meta.get("reporting_period"),
            },
            "headers": headers,
            "preview": preview_rows,
            "errors": errors,
        }

    def list_uploads(self, limit: int = 50, offset: int = 0) -> List[Upload]:
        """List recent uploads"""
        return (
            self.db.query(Upload)
            .order_by(Upload.upload_time.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )
