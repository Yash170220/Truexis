"""API endpoints for report export."""

import io
from datetime import datetime
from pathlib import Path
from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from src.api.auth import get_current_user
from src.common.database import get_db
from src.common.models import GeneratedReport, Upload, User
from src.reporting.generator import ReportGenerator

router = APIRouter(prefix="/api/v1/export", tags=["export"])

TEMPLATES_DIR = Path(__file__).parent.parent.parent / "data" / "templates"
generator = ReportGenerator(str(TEMPLATES_DIR))

# ── Brand colours ────────────────────────────────────────────
COPPER   = colors.HexColor("#B87333")
BROWN    = colors.HexColor("#3E2723")
MID      = colors.HexColor("#6D4C41")
TAUPE    = colors.HexColor("#D4C4B0")
BG_ALT   = colors.HexColor("#F5EFE6")
GOLD     = colors.HexColor("#D4AF37")
GRAY     = colors.HexColor("#8D8D8D")
WHITE    = colors.white
BLACK    = colors.black


def _build_pdf(
    upload: Upload,
    narratives: list[dict],
    framework: str,
) -> bytes:
    """Render narratives into a branded Truvexis PDF and return bytes."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=2.2 * cm,
        rightMargin=2.2 * cm,
        topMargin=2.5 * cm,
        bottomMargin=2.5 * cm,
    )

    styles = getSampleStyleSheet()

    # Custom styles
    brand_title = ParagraphStyle(
        "BrandTitle",
        fontName="Helvetica-Bold",
        fontSize=22,
        textColor=BROWN,
        spaceAfter=4,
        alignment=TA_LEFT,
    )
    brand_sub = ParagraphStyle(
        "BrandSub",
        fontName="Helvetica",
        fontSize=10,
        textColor=COPPER,
        spaceAfter=2,
        alignment=TA_LEFT,
        letterSpacing=1.5,
    )
    meta_label = ParagraphStyle(
        "MetaLabel",
        fontName="Helvetica",
        fontSize=7.5,
        textColor=GRAY,
        spaceAfter=1,
        leading=10,
    )
    meta_value = ParagraphStyle(
        "MetaValue",
        fontName="Helvetica-Bold",
        fontSize=9,
        textColor=BROWN,
        spaceAfter=6,
        leading=12,
    )
    indicator_heading = ParagraphStyle(
        "IndicatorHeading",
        fontName="Helvetica-Bold",
        fontSize=12,
        textColor=BROWN,
        spaceBefore=14,
        spaceAfter=4,
    )
    section_label = ParagraphStyle(
        "SectionLabel",
        fontName="Helvetica-Bold",
        fontSize=7.5,
        textColor=COPPER,
        spaceAfter=4,
        letterSpacing=1.2,
    )
    body_text = ParagraphStyle(
        "BodyText",
        fontName="Helvetica",
        fontSize=9.5,
        textColor=MID,
        spaceAfter=8,
        leading=15,
    )
    footer_text = ParagraphStyle(
        "FooterText",
        fontName="Helvetica",
        fontSize=7,
        textColor=GRAY,
        alignment=TA_CENTER,
    )

    meta = upload.file_metadata or {}
    facility = meta.get("facility_name", "—")
    period   = meta.get("reporting_period", "—")
    generated_on = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")

    story = []

    # ── Cover header ────────────────────────────────────────
    story.append(Paragraph("TRUVEXIS", brand_sub))
    story.append(Paragraph("ESG Report", brand_title))
    story.append(Spacer(1, 6))
    story.append(HRFlowable(width="100%", thickness=2, color=COPPER, spaceAfter=16))

    # Meta table
    meta_data = [
        ["Framework", framework,  "Facility", facility],
        ["Period",    period,      "Generated", generated_on],
    ]
    tbl = Table(meta_data, colWidths=[3 * cm, 7 * cm, 3 * cm, 4 * cm])
    tbl.setStyle(TableStyle([
        ("FONTNAME",    (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE",    (0, 0), (-1, -1), 8.5),
        ("TEXTCOLOR",  (0, 0), (0, -1),  GRAY),
        ("TEXTCOLOR",  (2, 0), (2, -1),  GRAY),
        ("TEXTCOLOR",  (1, 0), (1, -1),  BROWN),
        ("TEXTCOLOR",  (3, 0), (3, -1),  BROWN),
        ("FONTNAME",   (1, 0), (1, -1),  "Helvetica-Bold"),
        ("FONTNAME",   (3, 0), (3, -1),  "Helvetica-Bold"),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [BG_ALT, WHITE]),
        ("TOPPADDING",  (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 20))

    # ── Narratives ──────────────────────────────────────────
    # Group by indicator
    grouped: dict[str, list[dict]] = {}
    for n in narratives:
        grouped.setdefault(n.get("indicator", "Unknown"), []).append(n)

    for idx, (indicator, sections) in enumerate(grouped.items()):
        story.append(HRFlowable(width="100%", thickness=0.5, color=TAUPE, spaceAfter=6))
        num_label = f"{idx + 1:02d}"
        story.append(Paragraph(f"{num_label}  {indicator}", indicator_heading))

        for sec in sections:
            section_name = sec.get("section", "").replace("_", " ").upper()
            content      = sec.get("content", "").replace("\\n", "\n").strip()
            citations    = sec.get("citations", [])
            word_count   = sec.get("word_count", 0)

            if section_name:
                story.append(Paragraph(
                    f"{section_name} &nbsp;·&nbsp; {word_count} words",
                    section_label,
                ))

            # Replace newlines with paragraph breaks
            for para in content.split("\n"):
                para = para.strip()
                if para:
                    story.append(Paragraph(para, body_text))

            if citations:
                cit_parts = []
                for c in citations:
                    verified = "✓" if c.get("verified") else ""
                    cit_parts.append(
                        f'<font color="#8D8D8D">{c.get("reference","")} = {c.get("value","")} {verified}</font>'
                    )
                story.append(Paragraph(
                    "Citations: " + " &nbsp;|&nbsp; ".join(cit_parts),
                    ParagraphStyle("Cit", fontName="Helvetica", fontSize=7.5, textColor=GRAY, spaceAfter=10),
                ))

        story.append(Spacer(1, 8))

    # ── Footer ───────────────────────────────────────────────
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="100%", thickness=0.5, color=TAUPE, spaceAfter=6))
    story.append(Paragraph(
        f"Generated by Truvexis AI ESG Reporting · {generated_on} · Confidential",
        footer_text,
    ))

    doc.build(story)
    return buf.getvalue()


# ── Endpoints ────────────────────────────────────────────────

@router.get(
    "/{upload_id}/pdf",
    summary="Download generated ESG report as PDF",
    response_class=StreamingResponse,
)
async def download_pdf(
    upload_id: UUID,
    framework: str = Query("BRSR"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if upload.user_id is not None and upload.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    report = (
        db.query(GeneratedReport)
        .filter(
            GeneratedReport.upload_id == upload_id,
            GeneratedReport.framework == framework,
        )
        .order_by(GeneratedReport.created_at.desc())
        .first()
    )
    if not report:
        raise HTTPException(
            status_code=404,
            detail="No generated report found for this upload. Run generation first.",
        )

    pdf_bytes = _build_pdf(upload, report.narratives, framework)

    facility = (upload.file_metadata or {}).get("facility_name", "report")
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in facility)
    filename = f"Truvexis_{framework}_{safe_name}.pdf"

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/{upload_id}")
async def export_reports(
    upload_id: UUID,
    format: str = Query("docx", regex="^(docx|pdf|excel)$"),
    report_types: List[str] = Query(["brsr"], description="Report types to generate"),
):
    """Export reports in DOCX, PDF, or Excel format."""
    try:
        if format == "excel" and "brsr" in report_types:
            excel_path = generator.generate_brsr_excel(
                upload_id=upload_id,
                company_info={"name": "Company", "cin": "CIN123"},
                normalized_data=[],
            )
            return {
                "upload_id": str(upload_id),
                "format": "excel",
                "reports": [{"type": "brsr", "format": "excel", "filepath": excel_path}],
            }

        reports = generator.generate_reports(
            upload_id=upload_id,
            data={},
            output_dir=f"data/exports/{upload_id}",
            formats=report_types,
        )

        if format == "pdf":
            for report in reports:
                try:
                    pdf_path = generator.convert_to_pdf(report["filepath"])
                    report["pdf_filepath"] = pdf_path
                    report["format"] = "pdf"
                except Exception as e:
                    report["pdf_error"] = str(e)

        return {"upload_id": str(upload_id), "format": format, "reports": reports}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
