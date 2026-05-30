"""
Sandwich Club Think Tank — Branded PDF Formatter
FastAPI service that receives text content and produces
a formatted, branded PDF document.

Endpoints:
  POST /pdf/research-paper  → full research paper layout
  POST /pdf/article         → magazine article layout
  POST /pdf/press-release   → ANSA-style press release layout
  POST /pdf/comment         → person quote card layout
  GET  /pdf/download/{id}   → retrieve generated PDF
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import os
import uuid
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, HRFlowable,
    Table, TableStyle, PageBreak, KeepTogether
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ─── Config ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = "/tmp/sandwich_pdfs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Sandwich Club brand palette
BRAND = {
    "black": colors.HexColor("#0D0D0D"),
    "white": colors.HexColor("#FAFAFA"),
    "gold": colors.HexColor("#C9A84C"),
    "warm_gray": colors.HexColor("#F2EDE4"),
    "mid_gray": colors.HexColor("#8C8C8C"),
    "dark_gray": colors.HexColor("#2D2D2D"),
    "accent": colors.HexColor("#C9A84C"),
    "rule": colors.HexColor("#D4C5A0"),
}

app = FastAPI(title="Sandwich Club PDF Formatter", version="1.0.0")

# ─── Request Models ───────────────────────────────────────────────────────────

class PaperRequest(BaseModel):
    title: str
    content: str
    topic: str
    author: Optional[str] = "Sandwich Club Think Tank"
    date: Optional[str] = None
    subtitle: Optional[str] = None
    sandwich_index_score: Optional[float] = None
    data_context: Optional[str] = None

class ArticleRequest(BaseModel):
    title: str
    content: str
    topic: str
    author: Optional[str] = "Sandwicher Editorial"
    date: Optional[str] = None
    issue: Optional[str] = None

class PressReleaseRequest(BaseModel):
    title: str
    content: str
    topic: str
    embargo: Optional[str] = "FOR IMMEDIATE RELEASE"
    date: Optional[str] = None
    contact: Optional[str] = "press@sandwichclub.it"

class CommentRequest(BaseModel):
    person_name: str
    person_role: Optional[str] = ""
    comment_text: str
    topic: str
    date: Optional[str] = None

# ─── Style Builders ───────────────────────────────────────────────────────────

def get_base_styles():
    styles = getSampleStyleSheet()

    custom = {
        "cover_title": ParagraphStyle(
            "cover_title",
            fontName="Helvetica-Bold",
            fontSize=28,
            leading=34,
            textColor=BRAND["black"],
            spaceAfter=6,
            alignment=TA_LEFT,
        ),
        "cover_subtitle": ParagraphStyle(
            "cover_subtitle",
            fontName="Helvetica",
            fontSize=13,
            leading=18,
            textColor=BRAND["mid_gray"],
            spaceAfter=20,
            alignment=TA_LEFT,
        ),
        "section_head": ParagraphStyle(
            "section_head",
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=15,
            textColor=BRAND["black"],
            spaceBefore=16,
            spaceAfter=4,
            alignment=TA_LEFT,
        ),
        "body": ParagraphStyle(
            "body",
            fontName="Helvetica",
            fontSize=10,
            leading=15,
            textColor=BRAND["dark_gray"],
            spaceAfter=8,
            alignment=TA_JUSTIFY,
        ),
        "meta": ParagraphStyle(
            "meta",
            fontName="Helvetica",
            fontSize=8,
            leading=11,
            textColor=BRAND["mid_gray"],
            spaceAfter=4,
            alignment=TA_LEFT,
        ),
        "pullquote": ParagraphStyle(
            "pullquote",
            fontName="Helvetica-BoldOblique",
            fontSize=13,
            leading=18,
            textColor=BRAND["gold"],
            spaceBefore=12,
            spaceAfter=12,
            leftIndent=20,
            rightIndent=20,
            alignment=TA_LEFT,
        ),
        "label": ParagraphStyle(
            "label",
            fontName="Helvetica-Bold",
            fontSize=7,
            leading=10,
            textColor=BRAND["gold"],
            spaceAfter=4,
            alignment=TA_LEFT,
        ),
        "embargo": ParagraphStyle(
            "embargo",
            fontName="Helvetica-Bold",
            fontSize=9,
            leading=12,
            textColor=BRAND["white"],
            spaceAfter=0,
            alignment=TA_CENTER,
        ),
        "article_headline": ParagraphStyle(
            "article_headline",
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=27,
            textColor=BRAND["black"],
            spaceAfter=8,
            alignment=TA_LEFT,
        ),
        "article_deck": ParagraphStyle(
            "article_deck",
            fontName="Helvetica",
            fontSize=13,
            leading=18,
            textColor=BRAND["dark_gray"],
            spaceAfter=14,
            alignment=TA_LEFT,
        ),
        "person_name": ParagraphStyle(
            "person_name",
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=22,
            textColor=BRAND["black"],
            spaceAfter=4,
            alignment=TA_LEFT,
        ),
        "person_role": ParagraphStyle(
            "person_role",
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=BRAND["gold"],
            spaceAfter=16,
            alignment=TA_LEFT,
        ),
        "comment_body": ParagraphStyle(
            "comment_body",
            fontName="Helvetica-Oblique",
            fontSize=12,
            leading=18,
            textColor=BRAND["dark_gray"],
            spaceAfter=12,
            leftIndent=12,
            rightIndent=12,
            alignment=TA_JUSTIFY,
        ),
    }
    return custom


def header_footer(canvas, doc, doc_type="RESEARCH"):
    canvas.saveState()
    W, H = A4

    # Header bar
    canvas.setFillColor(BRAND["black"])
    canvas.rect(0, H - 18*mm, W, 18*mm, fill=1, stroke=0)

    # Header text — letter-spaced via manual spacing
    canvas.setFillColor(BRAND["gold"])
    canvas.setFont("Helvetica-Bold", 7)
    canvas.drawString(20*mm, H - 11*mm, "S A N D W I C H   C L U B   T H I N K   T A N K")

    canvas.setFillColor(BRAND["white"])
    canvas.setFont("Helvetica", 7)
    canvas.drawRightString(W - 20*mm, H - 11*mm, doc_type)

    # Footer rule
    canvas.setStrokeColor(BRAND["rule"])
    canvas.setLineWidth(0.5)
    canvas.line(20*mm, 18*mm, W - 20*mm, 18*mm)

    # Footer text
    canvas.setFillColor(BRAND["mid_gray"])
    canvas.setFont("Helvetica", 7)
    canvas.drawString(20*mm, 12*mm, "sandwichclub.it")
    canvas.drawCentredString(W/2, 12*mm, datetime.now().strftime("%B %Y"))
    canvas.drawRightString(W - 20*mm, 12*mm, f"Page {doc.page}")

    canvas.restoreState()


def parse_content_sections(content: str):
    """
    Parse markdown-ish content into (heading, body) tuples.
    Recognizes ## Section and ### Subsection headers.
    """
    sections = []
    current_head = None
    current_body = []

    for line in content.split("\n"):
        line = line.strip()
        if line.startswith("### "):
            if current_body:
                sections.append((current_head, "\n".join(current_body), "sub"))
            current_head = line[4:]
            current_body = []
        elif line.startswith("## "):
            if current_body:
                sections.append((current_head, "\n".join(current_body), "main"))
            current_head = line[3:]
            current_body = []
        elif line.startswith("# "):
            if current_body:
                sections.append((current_head, "\n".join(current_body), "main"))
            current_head = line[2:]
            current_body = []
        else:
            current_body.append(line)

    if current_body:
        sections.append((current_head, "\n".join(current_body), "main"))

    return sections


def build_gold_rule():
    return HRFlowable(
        width="100%",
        thickness=1.5,
        color=BRAND["gold"],
        spaceAfter=12,
        spaceBefore=4
    )

def build_thin_rule():
    return HRFlowable(
        width="100%",
        thickness=0.5,
        color=BRAND["rule"],
        spaceAfter=8,
        spaceBefore=4
    )


# ─── PDF Builders ────────────────────────────────────────────────────────────

def build_research_paper_pdf(req: PaperRequest, path: str):
    S = get_base_styles()
    date_str = req.date or datetime.now().strftime("%d %B %Y")

    doc = SimpleDocTemplate(
        path,
        pagesize=A4,
        topMargin=25*mm,
        bottomMargin=25*mm,
        leftMargin=22*mm,
        rightMargin=22*mm,
        title=req.title,
        author=req.author,
        subject="Sandwich Club Think Tank Research"
    )

    story = []

    # ── Cover block ──
    story.append(Spacer(1, 10*mm))
    story.append(Paragraph("THINK TANK RESEARCH", S["label"]))
    story.append(build_gold_rule())

    story.append(Paragraph(req.title, S["cover_title"]))
    if req.subtitle:
        story.append(Paragraph(req.subtitle, S["cover_subtitle"]))

    # Meta row
    meta_data = [
        [
            Paragraph(f"Topic: {req.topic}", S["meta"]),
            Paragraph(f"Date: {date_str}", S["meta"]),
            Paragraph(f"Author: {req.author}", S["meta"]),
        ]
    ]
    meta_table = Table(meta_data, colWidths=["33%", "33%", "34%"])
    meta_table.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BACKGROUND", (0, 0), (-1, -1), BRAND["warm_gray"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 6*mm))

    # Sandwich Index score block (if available)
    if req.sandwich_index_score is not None:
        score = req.sandwich_index_score
        if score >= 65:
            interpretation = "Positive — Expansion"
            score_color = colors.HexColor("#2D7A3A")
        elif score >= 50:
            interpretation = "Neutral — Stable"
            score_color = BRAND["gold"]
        else:
            interpretation = "Cautious — Contraction signals"
            score_color = colors.HexColor("#A0220E")

        idx_data = [[
            Paragraph("🥪 SANDWICH INDEX", S["label"]),
            Paragraph(f"<font color='#{score_color.hexval()[2:]}' size='16'><b>{score}</b></font>/100", S["meta"]),
            Paragraph(interpretation, S["meta"]),
        ]]
        idx_table = Table(idx_data, colWidths=["40%", "25%", "35%"])
        idx_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), BRAND["black"]),
            ("TEXTCOLOR", (0, 0), (-1, -1), BRAND["white"]),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(idx_table)
        story.append(Spacer(1, 4*mm))

    # Data context block
    if req.data_context:
        story.append(Paragraph("MARKET CONTEXT", S["label"]))
        story.append(Paragraph(req.data_context[:600] + "...", S["meta"]))
        story.append(build_thin_rule())

    # ── Body sections ──
    sections = parse_content_sections(req.content)
    for head, body, level in sections:
        if head:
            story.append(Paragraph(head.upper() if level == "main" else head, S["section_head"]))
            story.append(build_thin_rule())

        # Split body into paragraphs
        paras = [p.strip() for p in body.split("\n\n") if p.strip()]
        for i, para in enumerate(paras):
            if para.startswith(">") or para.startswith('"'):
                # Pullquote
                clean = para.lstrip(">").strip().strip('"')
                story.append(Paragraph(f'"{clean}"', S["pullquote"]))
            else:
                story.append(Paragraph(para, S["body"]))

        story.append(Spacer(1, 3*mm))

    # ── Footer block ──
    story.append(Spacer(1, 8*mm))
    story.append(build_gold_rule())
    footer_data = [[
        Paragraph("Sandwich Club Think Tank", S["label"]),
        Paragraph("sandwichclub.it", S["meta"]),
        Paragraph("© All rights reserved", S["meta"]),
    ]]
    footer_table = Table(footer_data, colWidths=["40%", "30%", "30%"])
    footer_table.setStyle(TableStyle([
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("ALIGN", (1, 0), (1, -1), "CENTER"),
        ("ALIGN", (2, 0), (2, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(footer_table)

    doc.build(
        story,
        onFirstPage=lambda c, d: header_footer(c, d, "THINK TANK RESEARCH"),
        onLaterPages=lambda c, d: header_footer(c, d, "THINK TANK RESEARCH")
    )


def build_article_pdf(req: ArticleRequest, path: str):
    S = get_base_styles()
    date_str = req.date or datetime.now().strftime("%d %B %Y")

    doc = SimpleDocTemplate(
        path,
        pagesize=A4,
        topMargin=25*mm,
        bottomMargin=25*mm,
        leftMargin=22*mm,
        rightMargin=22*mm,
        title=req.title,
    )

    story = []
    story.append(Spacer(1, 8*mm))

    # Issue label
    issue_label = req.issue or "SANDWICHER MAGAZINE"
    story.append(Paragraph(issue_label, S["label"]))

    # Headline
    story.append(build_gold_rule())
    story.append(Paragraph(req.title, S["article_headline"]))

    # Parse first paragraph as deck
    sections = parse_content_sections(req.content)
    body_sections = sections.copy()

    if body_sections:
        first_head, first_body, _ = body_sections[0]
        paras = [p.strip() for p in first_body.split("\n\n") if p.strip()]
        if paras and not first_head:
            story.append(Paragraph(paras[0], S["article_deck"]))
            # Remove first paragraph from body
            remaining = "\n\n".join(paras[1:])
            body_sections[0] = (first_head, remaining, "main")

    # Byline
    story.append(Paragraph(f"By {req.author}  ·  {date_str}", S["meta"]))
    story.append(build_thin_rule())
    story.append(Spacer(1, 4*mm))

    # Body
    for head, body, level in body_sections:
        if head:
            story.append(Paragraph(head, S["section_head"]))

        paras = [p.strip() for p in body.split("\n\n") if p.strip()]
        for para in paras:
            if para.startswith(">") or (para.startswith('"') and len(para) < 300):
                story.append(Paragraph(f'"{para.lstrip(">").strip()}"', S["pullquote"]))
            else:
                story.append(Paragraph(para, S["body"]))

        story.append(Spacer(1, 2*mm))

    doc.build(
        story,
        onFirstPage=lambda c, d: header_footer(c, d, "SANDWICHER"),
        onLaterPages=lambda c, d: header_footer(c, d, "SANDWICHER")
    )


def build_press_release_pdf(req: PressReleaseRequest, path: str):
    S = get_base_styles()
    date_str = req.date or datetime.now().strftime("%d %B %Y")

    doc = SimpleDocTemplate(
        path,
        pagesize=A4,
        topMargin=25*mm,
        bottomMargin=25*mm,
        leftMargin=22*mm,
        rightMargin=22*mm,
        title=f"PRESS RELEASE: {req.title}",
    )

    story = []
    story.append(Spacer(1, 8*mm))

    # Embargo banner
    embargo_data = [[Paragraph(req.embargo, S["embargo"])]]
    embargo_table = Table(embargo_data, colWidths=["100%"])
    embargo_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND["black"]),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(embargo_table)
    story.append(Spacer(1, 6*mm))

    # Label + rule
    story.append(Paragraph("COMUNICATO STAMPA  ·  PRESS RELEASE", S["label"]))
    story.append(build_gold_rule())

    # Headline
    story.append(Paragraph(req.title, S["cover_title"]))
    story.append(Spacer(1, 2*mm))

    # Dateline
    story.append(Paragraph(f"Milano/Roma, {date_str}", S["meta"]))
    story.append(build_thin_rule())
    story.append(Spacer(1, 4*mm))

    # Body
    sections = parse_content_sections(req.content)
    for head, body, level in sections:
        if head and head.lower() not in ["press release", "comunicato stampa"]:
            story.append(Paragraph(head.upper(), S["section_head"]))

        paras = [p.strip() for p in body.split("\n\n") if p.strip()]
        for para in paras:
            if '"' in para and len(para) < 500 and para.count('"') >= 2:
                story.append(Paragraph(para, S["pullquote"]))
            else:
                story.append(Paragraph(para, S["body"]))

        story.append(Spacer(1, 2*mm))

    # Contact footer
    story.append(Spacer(1, 8*mm))
    story.append(build_gold_rule())

    contact_data = [[
        Paragraph("MEDIA CONTACT", S["label"]),
        Paragraph(req.contact, S["body"]),
    ]]
    contact_table = Table(contact_data, colWidths=["30%", "70%"])
    contact_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND["warm_gray"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(contact_table)

    doc.build(
        story,
        onFirstPage=lambda c, d: header_footer(c, d, "PRESS RELEASE"),
        onLaterPages=lambda c, d: header_footer(c, d, "PRESS RELEASE")
    )


def build_comment_pdf(req: CommentRequest, path: str):
    S = get_base_styles()
    date_str = req.date or datetime.now().strftime("%d %B %Y")

    doc = SimpleDocTemplate(
        path,
        pagesize=A4,
        topMargin=25*mm,
        bottomMargin=25*mm,
        leftMargin=22*mm,
        rightMargin=22*mm,
        title=f"Comment: {req.person_name}",
    )

    story = []
    story.append(Spacer(1, 10*mm))

    story.append(Paragraph("STATEMENT / COMMENT", S["label"]))
    story.append(build_gold_rule())

    story.append(Paragraph(req.person_name, S["person_name"]))
    if req.person_role:
        story.append(Paragraph(req.person_role, S["person_role"]))

    story.append(Paragraph(f"On: {req.topic}  ·  {date_str}", S["meta"]))
    story.append(Spacer(1, 6*mm))

    # Large opening quote mark (simulated)
    story.append(Paragraph(
        '<font size="36" color="#C9A84C">❝</font>',
        ParagraphStyle("q", fontName="Helvetica", fontSize=36, leading=40, textColor=BRAND["gold"])
    ))

    # Comment body
    paras = [p.strip() for p in req.comment_text.split("\n\n") if p.strip()]
    for para in paras:
        story.append(Paragraph(para, S["comment_body"]))

    story.append(Spacer(1, 4*mm))
    story.append(build_thin_rule())
    story.append(Paragraph(f"— {req.person_name}", S["meta"]))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(
        "Comment generated by Sandwich Club Think Tank research pipeline.",
        S["meta"]
    ))

    doc.build(
        story,
        onFirstPage=lambda c, d: header_footer(c, d, "STATEMENT"),
        onLaterPages=lambda c, d: header_footer(c, d, "STATEMENT")
    )


# ─── API Routes ───────────────────────────────────────────────────────────────

@app.post("/pdf/research-paper")
async def create_research_paper(req: PaperRequest):
    pdf_id = str(uuid.uuid4())
    path = os.path.join(OUTPUT_DIR, f"{pdf_id}.pdf")
    try:
        build_research_paper_pdf(req, path)
        return {"pdf_id": pdf_id, "filename": f"sandwich_research_{datetime.now().strftime('%Y%m%d')}.pdf"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pdf/article")
async def create_article(req: ArticleRequest):
    pdf_id = str(uuid.uuid4())
    path = os.path.join(OUTPUT_DIR, f"{pdf_id}.pdf")
    try:
        build_article_pdf(req, path)
        return {"pdf_id": pdf_id, "filename": f"sandwicher_article_{datetime.now().strftime('%Y%m%d')}.pdf"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pdf/press-release")
async def create_press_release(req: PressReleaseRequest):
    pdf_id = str(uuid.uuid4())
    path = os.path.join(OUTPUT_DIR, f"{pdf_id}.pdf")
    try:
        build_press_release_pdf(req, path)
        return {"pdf_id": pdf_id, "filename": f"sandwich_press_{datetime.now().strftime('%Y%m%d')}.pdf"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pdf/comment")
async def create_comment(req: CommentRequest):
    pdf_id = str(uuid.uuid4())
    path = os.path.join(OUTPUT_DIR, f"{pdf_id}.pdf")
    try:
        build_comment_pdf(req, path)
        return {"pdf_id": pdf_id, "filename": f"sandwich_comment_{req.person_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pdf/download/{pdf_id}")
async def download_pdf(pdf_id: str):
    path = os.path.join(OUTPUT_DIR, f"{pdf_id}.pdf")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="PDF not found")
    return FileResponse(path, media_type="application/pdf", filename=f"{pdf_id}.pdf")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "Sandwich Club PDF Formatter"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
