import re
import os
import sys
from pathlib import Path
from html import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, Preformatted, PageBreak
from reportlab.lib.enums import TA_LEFT


# ---------------------------------------------------------------------------
# Configure fonts
# ---------------------------------------------------------------------------
NotoSansKR = Path(r"C:\Windows\Fonts\NotoSansKR-VF.ttf")
DejaVuSansMono = Path(r"C:\Users\zapan\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\LocalCache\local-packages\Python310\site-packages\matplotlib\mpl-data\fonts\ttf\DejaVuSansMono.ttf")

if not NotoSansKR.exists():
    raise FileNotFoundError(f"Required font not found: {NotoSansKR}")
if not DejaVuSansMono.exists():
    raise FileNotFoundError(f"Required mono font not found: {DejaVuSansMono}")

pdfmetrics.registerFont(TTFont('NotoSansKR', str(NotoSansKR)))
pdfmetrics.registerFont(TTFont('NotoSansKR-Bold', str(NotoSansKR)))
pdfmetrics.registerFont(TTFont('DejaVuSansMono', str(DejaVuSansMono)))

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='KRBody', parent=styles['BodyText'], fontName='NotoSansKR', fontSize=10.5, leading=15, textColor=colors.black))
styles.add(ParagraphStyle(name='KRH1', parent=styles['Title'], fontName='NotoSansKR-Bold', fontSize=22, leading=26, spaceBefore=20, spaceAfter=12, textColor=colors.black))
styles.add(ParagraphStyle(name='KRH2', parent=styles['Heading2'], fontName='NotoSansKR-Bold', fontSize=17, leading=22, spaceBefore=14, spaceAfter=8, textColor=colors.black))
styles.add(ParagraphStyle(name='KRH3', parent=styles['Heading3'], fontName='NotoSansKR-Bold', fontSize=14, leading=18, spaceBefore=10, spaceAfter=6, textColor=colors.black))
styles.add(ParagraphStyle(name='KRCaption', parent=styles['BodyText'], fontName='NotoSansKR', fontSize=9, leading=12, italic=True, textColor=colors.grey))
styles.add(ParagraphStyle(name='KRList', parent=styles['BodyText'], fontName='NotoSansKR', fontSize=10.5, leading=14, leftIndent=14, bulletIndent=14))
styles.add(ParagraphStyle(name='KRCode', parent=styles['Code'], fontName='DejaVuSansMono', fontSize=8.5, leading=11, leftIndent=8, rightIndent=8, textColor=colors.black))

body_style = styles['KRBody']


def md_inline_to_reportlab(line):
    # No HTML injection; escape content, then create allowed simple reportlab markup.
    text = escape(line)
    # Replace inline code
    text = re.sub(r'`([^`]+)`', r'<font face="DejaVuSansMono">\1</font>', text)
    # Bold/italic emphasis
    text = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*([^*]+)\*', r'<i>\1</i>', text)
    # Markdown links -> text only
    text = re.sub(r'\[([^\]]+)\]\(([^\)]+)\)', r'\1', text)
    return text


def is_table_start(line):
    return line.strip().startswith('|')


def parse_table(lines, start):
    # Gather rows until blank or non-table row.
    rows = []
    i = start
    while i < len(lines):
        line = lines[i]
        if not is_table_start(line):
            break
        if line.strip().startswith('|---'):
            # separator line, skip
            i += 1
            continue
        parts = [p.strip() for p in line.strip().strip('|').split('|')]
        rows.append(parts)
        i += 1
    # The first row is header, rest is body.
    # Preserve as rows with column widths.
    # Use all columns from max width.
    if not rows:
        return [], i
    # drop header separators row if present
    data = []
    for row in rows:
        # skip separator rows if any
        if set(row) == {'---'} or all(re.fullmatch(r':?-{3,}:?', cell) for cell in row):
            continue
        # make short
        data.append(row)
    if not data:
        return [], i
    return data, i


def make_table(story, rows):
    if not rows or len(rows) == 0:
        return
    # Distinguish header row from data
    header = rows[0]
    body = rows[1:]
    data = [header]
    for r in body:
        data.append(r)
    # Keep simple and readable; force header cell color.
    table = Table(data, colWidths=[None] * len(header))
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e7eaf0')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('FONTNAME', (0, 0), (-1, 0), 'NotoSansKR-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'NotoSansKR'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('WORDWRAP', (0, 0), (-1, -1), 1),
    ]))
    story.append(table)
    story.append(Spacer(1, 8*mm))


def convert_line_to_story(story, line, doc_dir, line_no=0):
    # Fenced code block
    if line.startswith('```'):
        # handled externally with state machine below
        return

    # Headings
    h = re.match(r'^(#{1,3})\s+(.+?)\s*$', line)
    if h:
        level = len(h.group(1))
        text = h.group(2).strip()
        # Normalized heading paragraph styles
        if level == 1:
            story.append(Paragraph(md_inline_to_reportlab(text), styles['KRH1']))
        elif level == 2:
            story.append(Paragraph(md_inline_to_reportlab(text), styles['KRH2']))
        elif level == 3:
            story.append(Paragraph(md_inline_to_reportlab(text), styles['KRH3']))
        return

    # Image
    img = re.match(r'^!\[([^\]]*)\]\(([^\)]+)\)$', line)
    if img:
        path = (doc_dir / img.group(2)).resolve()
        if path.exists():
            try:
                img_obj = Image(path, width=120*mm, height=80*mm)
                story.append(img_obj)
                story.append(Spacer(1, 5*mm))
                story.append(Paragraph(md_inline_to_reportlab(img.group(1)), styles['KRCaption']))
                story.append(Spacer(1, 8*mm))
            except Exception as exc:
                story.append(Paragraph(f'[image conversion skipped: {exc}]', body_style))
        return

    # Lists
    if line.startswith('- ') or line.startswith('* '):
        story.append(Paragraph(f'• {md_inline_to_reportlab(line[2:].strip())}', styles['KRList']))
        return

    # Code blocks handled by state machine elsewhere.
    # Regular paragraph
    if line.strip():
        story.append(Paragraph(md_inline_to_reportlab(line.strip()), body_style))


def convert_markdown_to_pdf(md_path: Path, pdf_path: Path):
    text = md_path.read_text(encoding='utf-8')
    lines = text.splitlines()
    story = []
    doc_dir = md_path.parent

    # initial page object
    # parse state: code fence and table
    i = 0
    while i < len(lines):
        line = lines[i]

        # Code fence
        if line.strip().startswith('```'):
            fence = line.strip()
            fence_lang = fence[3:].strip()
            code_lines = []
            i += 1
            while i < len(lines):
                if lines[i].strip().startswith('```'):
                    break
                code_lines.append(lines[i])
                i += 1
            # emit code block; include fence language ignored
            code_text = '\n'.join(code_lines)
            # if fence has language=sql/yaml/json etc all code with DejaVuSansMono
            story.append(Preformatted(code_text, styles['KRCode']))
            story.append(Spacer(1, 5*mm))
            i += 1
            continue

        # Image
        if line.strip().startswith('!['):
            match = re.match(r'^!\[([^\]]*)\]\(([^\)]+)\)$', line.strip())
            if match:
                img_path = (doc_dir / match.group(2)).resolve()
                if img_path.exists():
                    try:
                        # Fit width; keep an 80%-safe image size.
                        img = Image(img_path, width=130*mm, height=80*mm)
                        story.append(img)
                        story.append(Spacer(1, 5*mm))
                        story.append(Paragraph(match.group(1), styles['KRCaption']))
                    except Exception:
                        pass
                i += 1
                continue

        # New table block
        if is_table_start(line):
            # Gather lines until next non-table row; skip the separator line.
            table_lines = [line]
            j = i + 1
            while j < len(lines):
                l = lines[j]
                if not is_table_start(l):
                    break
                table_lines.append(l)
                j += 1
            # parse all table rows lines into list of lists from tilde/dashes etc
            rows = []
            for tl in table_lines:
                tl = tl.strip()
                if tl.startswith('|') and tl.endswith('|'):
                    # skip separators
                    if re.fullmatch(r'\|?\s*:?[-: ]+\|?\s*:?[-: ]+', tl):
                        continue
                    # ignore header sep cells
                    if tl.startswith('|---'):
                        continue
                    cells = [c.strip() for c in tl.strip('|').split('|')]
                    # skip first if blank? just use row.
                    if cells and all(not c for c in cells):
                        continue
                    rows.append(cells)
            if rows:
                # Use markdown table structure as data. Need header same as rows. If HTML? We'll keep only textual row data.
                # Add table
                make_table(story, rows)
                i = j
                continue

        # Heading
        heading = re.match(r'^(#{1,3})\s+(.+?)\s*$', line)
        if heading:
            level = len(heading.group(1))
            text = heading.group(2).strip()
            if level == 1:
                story.append(Paragraph(md_inline_to_reportlab(text), styles['KRH1']))
            elif level == 2:
                story.append(Paragraph(md_inline_to_reportlab(text), styles['KRH2']))
            elif level == 3:
                story.append(Paragraph(md_inline_to_reportlab(text), styles['KRH3']))
            i += 1
            continue

        # Bullets
        if line.startswith('- ') or line.startswith('* '):
            story.append(Paragraph('• ' + md_inline_to_reportlab(line[2:].strip()), styles['KRList']))
            i += 1
            continue

        # Blockquote or fence? otherwise paragraph
        if line.strip():
            story.append(Paragraph(md_inline_to_reportlab(line.strip()), body_style))

        i += 1

    # Use A4 paper; set margins.
    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4, leftMargin=16*mm, rightMargin=16*mm, topMargin=14*mm, bottomMargin=14*mm)
    doc.build(story)


if __name__ == '__main__':
    md_path = Path(r"C:\Users\zapan\Documents\poc-marketdata\docs\architecture-poc-integrated-guide.md")
    pdf_path = Path(r"C:\Users\zapan\Documents\poc-marketdata\docs\architecture-poc-integrated-guide.pdf")
    convert_markdown_to_pdf(md_path, pdf_path)
    print(f"Created PDF: {pdf_path}")
