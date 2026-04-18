import { useState, useCallback } from 'react';
import { Download, Loader } from 'lucide-react';
import jsPDF from 'jspdf';
import html2canvas from 'html2canvas';

/* ─── Brand colours (match CSS tokens) ─────────────────────────────────────── */
const C = {
  bg:         [5,   8,  20],   // #050814
  surface:    [13,  18,  36],  // #0d1224
  purple:     [124, 58, 237],  // --primary
  cyan:       [6,  182, 212],  // --secondary
  green:      [16, 185, 129],  // --success
  yellow:     [245, 158, 11],  // --warning
  red:        [239, 68,  68],  // --danger
  white:      [241, 245, 249], // --text
  muted:      [148, 163, 184], // --text-muted
  faint:      [71,  85, 105],  // --text-faint
  border:     [30,  41,  59],  // slightly lighter than bg
};

/* ─── Helpers ───────────────────────────────────────────────────────────────── */
function hexToRgb(arr) { return { r: arr[0], g: arr[1], b: arr[2] }; }

function setFill(doc, arr)   { doc.setFillColor(...arr); }
function setDraw(doc, arr)   { doc.setDrawColor(...arr); }
function setTxt(doc, arr)    { doc.setTextColor(...arr); }

function rect(doc, x, y, w, h, arr, style = 'F') {
  setFill(doc, arr);
  doc.rect(x, y, w, h, style);
}

function gradientRect(doc, x, y, w, h) {
  // Approximate gradient with thin strips
  const steps = 40;
  for (let i = 0; i < steps; i++) {
    const t = i / steps;
    const r = Math.round(C.purple[0] + (C.cyan[0] - C.purple[0]) * t);
    const g = Math.round(C.purple[1] + (C.cyan[1] - C.purple[1]) * t);
    const b = Math.round(C.purple[2] + (C.cyan[2] - C.purple[2]) * t);
    doc.setFillColor(r, g, b);
    doc.rect(x + (w / steps) * i, y, w / steps + 0.5, h, 'F');
  }
}

/* ─── Page management ───────────────────────────────────────────────────────── */
const PAGE_W = 210; // A4 mm
const PAGE_H = 297;
const MARGIN  = 18;
const CONTENT_W = PAGE_W - MARGIN * 2;

function addPageBg(doc) {
  rect(doc, 0, 0, PAGE_W, PAGE_H, C.bg);
  // Subtle purple glow top-left
  doc.setFillColor(124, 58, 237, 0.08);
  doc.circle(30, 30, 60, 'F');
  // Subtle cyan glow bottom-right
  doc.setFillColor(6, 182, 212, 0.06);
  doc.circle(PAGE_W - 20, PAGE_H - 20, 50, 'F');
}

class PDFWriter {
  constructor(doc) {
    this.doc = doc;
    this.page = 1;
    this.y = MARGIN;
    addPageBg(doc);
  }

  newPage() {
    this.doc.addPage();
    this.page++;
    addPageBg(this.doc);
    this.y = MARGIN + 10;
  }

  ensureSpace(needed) {
    if (this.y + needed > PAGE_H - 22) this.newPage();
  }

  // Draw footer on all pages
  drawFooter(totalPages, datasetName) {
    const { doc } = this;
    for (let p = 1; p <= totalPages; p++) {
      doc.setPage(p);
      // Footer bar
      rect(doc, 0, PAGE_H - 14, PAGE_W, 14, C.surface);
      gradientRect(doc, 0, PAGE_H - 14, PAGE_W, 2);

      doc.setFontSize(7.5);
      setTxt(doc, C.faint);
      doc.text(`DataLens Report  ·  ${datasetName}  ·  Confidential`, MARGIN, PAGE_H - 6);
      doc.text(`Page ${p} of ${totalPages}`, PAGE_W - MARGIN, PAGE_H - 6, { align: 'right' });
    }
    doc.setPage(this.page); // restore last page
  }

  // Horizontal rule
  hr(color = C.border) {
    this.ensureSpace(4);
    setDraw(this.doc, color);
    this.doc.setLineWidth(0.3);
    this.doc.line(MARGIN, this.y, PAGE_W - MARGIN, this.y);
    this.y += 5;
  }

  // Gap
  gap(mm = 4) { this.y += mm; }

  // Text with colour + optional bold
  text(txt, x, opts = {}) {
    const { size = 10, color = C.white, bold = false, maxWidth, align } = opts;
    this.doc.setFontSize(size);
    this.doc.setFont('helvetica', bold ? 'bold' : 'normal');
    setTxt(this.doc, color);
    const tOpts = {};
    if (maxWidth) tOpts.maxWidth = maxWidth;
    if (align)    tOpts.align   = align;
    this.doc.text(txt, x, this.y, tOpts);
    // Estimate line height
    const lineH = size * 0.41;
    const lines = maxWidth ? this.doc.splitTextToSize(txt, maxWidth) : [txt];
    this.y += lineH * lines.length + 1.5;
    return lines.length;
  }

  // Wrapped paragraph that auto-paginates
  para(txt, opts = {}) {
    if (!txt) return;
    const { size = 10, color = C.white } = opts;
    this.doc.setFontSize(size);
    this.doc.setFont('helvetica', 'normal');
    const lines = this.doc.splitTextToSize(String(txt), CONTENT_W);
    const lineH = size * 0.41 + 0.5;
    for (const line of lines) {
      this.ensureSpace(lineH + 2);
      setTxt(this.doc, color);
      this.doc.text(line, MARGIN, this.y);
      this.y += lineH;
    }
    this.y += 2;
  }

  // Badge-style label pill
  pill(txt, x, bgColor, txtColor) {
    const { doc } = this;
    doc.setFontSize(7.5);
    doc.setFont('helvetica', 'bold');
    const w = doc.getTextWidth(txt) + 6;
    const h = 5.5;
    setFill(doc, bgColor);
    doc.roundedRect(x, this.y - 4, w, h, 1.5, 1.5, 'F');
    setTxt(doc, txtColor);
    doc.text(txt, x + 3, this.y - 0.3);
    return x + w + 3;
  }

  // Section heading (Q1, Q2 …)
  sectionHeading(label, qText) {
    this.ensureSpace(18);
    // Full-width background band
    rect(this.doc, 0, this.y - 4, PAGE_W, 14, C.surface);
    gradientRect(this.doc, MARGIN - 4, this.y - 3, 3, 11);

    this.doc.setFontSize(8);
    this.doc.setFont('helvetica', 'bold');
    setTxt(this.doc, C.purple);
    this.doc.text(label, MARGIN + 2, this.y + 2);

    const labelW = this.doc.getTextWidth(label);
    this.doc.setFontSize(10);
    this.doc.setFont('helvetica', 'bold');
    setTxt(this.doc, C.white);
    const qLines = this.doc.splitTextToSize(qText, CONTENT_W - labelW - 6);
    this.doc.text(qLines[0] || '', MARGIN + labelW + 5, this.y + 2);
    this.y += 12;
  }

  // Sub-label (EXPLANATION, INSIGHTS …)
  subLabel(txt) {
    this.ensureSpace(8);
    this.doc.setFontSize(7.5);
    this.doc.setFont('helvetica', 'bold');
    setTxt(this.doc, C.faint);
    this.doc.text(txt.toUpperCase(), MARGIN, this.y);
    this.y += 5;
  }

  // Bullet list
  bullets(items) {
    if (!items?.length) return;
    this.doc.setFontSize(9.5);
    this.doc.setFont('helvetica', 'normal');
    for (const item of items) {
      const lines = this.doc.splitTextToSize(String(item), CONTENT_W - 6);
      const lineH = 9.5 * 0.41;
      const needed = lines.length * (lineH + 0.5) + 3;
      this.ensureSpace(needed);
      // Gradient dot
      gradientRect(this.doc, MARGIN, this.y - 2, 2.5, 2.5);
      setTxt(this.doc, C.white);
      for (let i = 0; i < lines.length; i++) {
        this.doc.text(lines[i], MARGIN + 5, this.y + i * (lineH + 0.5));
      }
      this.y += lines.length * (lineH + 0.5) + 2;
    }
    this.y += 1;
  }

  // "Why" analysis block
  whyBlock(txt) {
    if (!txt) return;
    this.ensureSpace(18);
    const lines = this.doc.splitTextToSize(String(txt), CONTENT_W - 8);
    const lineH = 9.5 * 0.41 + 0.5;
    const blockH = lines.length * lineH + 10;
    this.ensureSpace(blockH);
    // Background
    doc_rect_rounded(this.doc, MARGIN, this.y - 2, CONTENT_W, blockH, 2, C.surface);
    setDraw(this.doc, C.cyan);
    this.doc.setLineWidth(0.4);
    this.doc.roundedRect(MARGIN, this.y - 2, CONTENT_W, blockH, 2, 2, 'S');
    // Label
    this.doc.setFontSize(7.5);
    this.doc.setFont('helvetica', 'bold');
    setTxt(this.doc, C.cyan);
    this.doc.text('🔍 WHY DID THIS HAPPEN?', MARGIN + 4, this.y + 4);
    this.y += 8;
    // Body
    this.doc.setFontSize(9.5);
    this.doc.setFont('helvetica', 'normal');
    setTxt(this.doc, C.white);
    for (const line of lines) {
      this.doc.text(line, MARGIN + 4, this.y);
      this.y += lineH;
    }
    this.y += 5;
  }

  // Embed screenshot image
  async embedImage(dataUrl, maxH = 70) {
    if (!dataUrl) return;
    // Try to fit — if not, new page
    this.ensureSpace(maxH + 8);
    const imgProps = this.doc.getImageProperties(dataUrl);
    const scale = Math.min(CONTENT_W / imgProps.width, maxH / imgProps.height);
    const w = imgProps.width * scale;
    const h = imgProps.height * scale;
    const x = MARGIN + (CONTENT_W - w) / 2; // centre
    // Border
    setDraw(this.doc, C.border);
    this.doc.setLineWidth(0.3);
    this.doc.roundedRect(x - 1, this.y - 1, w + 2, h + 2, 2, 2, 'S');
    this.doc.addImage(dataUrl, 'PNG', x, this.y, w, h);
    this.y += h + 6;
  }

  // Result data table
  dataTable(rows, columns) {
    if (!rows?.length || !columns?.length) return;
    const visibleCols = columns.slice(0, 6); // cap columns
    const visibleRows = rows.slice(0, 15);   // cap rows
    const colW = CONTENT_W / visibleCols.length;
    const rowH = 6;
    const headerH = 7;

    this.ensureSpace(headerH + rowH * Math.min(visibleRows.length, 6) + 4);

    // Header
    gradientRect(this.doc, MARGIN, this.y, CONTENT_W, headerH);
    this.doc.setFontSize(7.5);
    this.doc.setFont('helvetica', 'bold');
    setTxt(this.doc, C.white);
    visibleCols.forEach((col, i) => {
      const cellX = MARGIN + colW * i;
      this.doc.text(String(col).toUpperCase(), cellX + 2, this.y + 4.5, { maxWidth: colW - 3 });
    });
    this.y += headerH;

    // Rows
    for (let ri = 0; ri < visibleRows.length; ri++) {
      this.ensureSpace(rowH + 2);
      const bgColor = ri % 2 === 0 ? C.bg : C.surface;
      rect(this.doc, MARGIN, this.y, CONTENT_W, rowH, bgColor);

      this.doc.setFontSize(7.5);
      this.doc.setFont('helvetica', 'normal');
      setTxt(this.doc, ri % 2 === 0 ? C.muted : C.white);
      visibleCols.forEach((col, ci) => {
        const val = visibleRows[ri][col];
        const formatted = val === null || val === undefined ? '—' : String(val);
        this.doc.text(formatted, MARGIN + colW * ci + 2, this.y + rowH - 1.5, { maxWidth: colW - 3 });
      });
      this.y += rowH;
    }

    if (rows.length > 15) {
      this.gap(2);
      this.doc.setFontSize(7.5);
      this.doc.setFont('helvetica', 'italic');
      setTxt(this.doc, C.faint);
      this.doc.text(`… and ${rows.length - 15} more rows`, MARGIN, this.y);
      this.y += 4;
    }
    this.y += 4;
  }

  // Preprocessing log
  preprocLog(log) {
    if (!log?.length) return;
    this.doc.setFontSize(8);
    this.doc.setFont('helvetica', 'normal');
    for (const line of log) {
      this.ensureSpace(5);
      setTxt(this.doc, C.green);
      this.doc.text(String(line), MARGIN, this.y, { maxWidth: CONTENT_W });
      this.y += 4.5;
    }
    this.y += 2;
  }
}

/* ─── Helper for filled rounded rects (jsPDF doesn't do fillColor+roundedRect together natively) */
function doc_rect_rounded(doc, x, y, w, h, r, color) {
  setFill(doc, color);
  doc.roundedRect(x, y, w, h, r, r, 'F');
}

/* ─── Screenshot a DOM element ──────────────────────────────────────────────── */
async function screenshotElement(el) {
  if (!el) return null;
  try {
    const canvas = await html2canvas(el, {
      backgroundColor: '#050814',
      scale: 2,
      useCORS: true,
      logging: false,
    });
    return canvas.toDataURL('image/png');
  } catch {
    return null;
  }
}

/* ─── Cover page ────────────────────────────────────────────────────────────── */
function drawCoverPage(writer, dataset, mode, dataHealth, aiCount) {
  const { doc } = writer;

  // Big gradient banner
  gradientRect(doc, 0, 0, PAGE_W, 52);

  // Logo text
  doc.setFontSize(22);
  doc.setFont('helvetica', 'bold');
  setTxt(doc, C.white);
  doc.text('DataLens', MARGIN, 24);

  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  setTxt(doc, [220, 220, 255]);
  doc.text('AI-Powered CSV Analytics Report', MARGIN, 33);

  // Date
  const now = new Date();
  const dateStr = now.toLocaleString('en-IN', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
  doc.setFontSize(8);
  setTxt(doc, [200, 200, 240]);
  doc.text(dateStr, PAGE_W - MARGIN, 33, { align: 'right' });

  writer.y = 62;

  // Dataset card
  rect(doc, MARGIN, writer.y, CONTENT_W, 44, C.surface);
  setDraw(doc, C.purple);
  doc.setLineWidth(0.5);
  doc.roundedRect(MARGIN, writer.y, CONTENT_W, 44, 3, 3, 'S');
  gradientRect(doc, MARGIN, writer.y, 4, 44);

  doc.setFontSize(8.5);
  doc.setFont('helvetica', 'bold');
  setTxt(doc, C.muted);
  doc.text('DATASET', MARGIN + 10, writer.y + 8);

  doc.setFontSize(14);
  doc.setFont('helvetica', 'bold');
  setTxt(doc, C.white);
  const nameShort = (dataset?.filename || 'Untitled').slice(0, 40);
  doc.text(nameShort, MARGIN + 10, writer.y + 18);

  // Stats row
  const stats = [
    ['Rows', (dataset?.row_count || 0).toLocaleString()],
    ['Columns', (dataset?.columns?.length || 0).toString()],
    ['Mode', mode === 'smart' ? '⚡ Smart' : mode === 'scalable' ? '🧠 Scalable' : '⚪ Raw'],
    ['Queries', aiCount.toString()],
  ];
  stats.forEach(([label, val], i) => {
    const x = MARGIN + 10 + i * 44;
    doc.setFontSize(7.5);
    doc.setFont('helvetica', 'normal');
    setTxt(doc, C.muted);
    doc.text(label, x, writer.y + 29);
    doc.setFontSize(11);
    doc.setFont('helvetica', 'bold');
    setTxt(doc, C.cyan);
    doc.text(String(val), x, writer.y + 37);
  });
  writer.y += 54;

  // Data health panel
  if (dataHealth) {
    writer.gap(4);
    const panelY = writer.y;
    rect(doc, MARGIN, panelY, CONTENT_W, 36, C.surface);
    setDraw(doc, C.border);
    doc.setLineWidth(0.3);
    doc.roundedRect(MARGIN, panelY, CONTENT_W, 36, 2, 2, 'S');

    doc.setFontSize(8);
    doc.setFont('helvetica', 'bold');
    setTxt(doc, C.muted);
    doc.text('DATA HEALTH', MARGIN + 6, panelY + 8);

    const hStats = [
      ['Missing', `${(dataHealth.missing_pct || 0).toFixed(1)}%`,
        dataHealth.missing_pct < 5 ? C.green : dataHealth.missing_pct < 20 ? C.yellow : C.red],
      ['Outliers', String(dataHealth.outliers || 0),
        (dataHealth.outliers || 0) > 0 ? C.yellow : C.green],
      ['Rows Used', (dataHealth.rows_used || 0) >= 1000
        ? `${((dataHealth.rows_used || 0) / 1000).toFixed(1)}k`
        : String(dataHealth.rows_used || 0), C.cyan],
      ['Confidence', `${dataHealth.confidence || 0}%`,
        dataHealth.confidence >= 80 ? C.green : dataHealth.confidence >= 60 ? C.yellow : C.red],
    ];
    hStats.forEach(([label, val, color], i) => {
      const x = MARGIN + 6 + i * 44;
      doc.setFontSize(7.5);
      doc.setFont('helvetica', 'normal');
      setTxt(doc, C.muted);
      doc.text(label, x, panelY + 18);
      doc.setFontSize(12);
      doc.setFont('helvetica', 'bold');
      setTxt(doc, color);
      doc.text(val, x, panelY + 28);
    });

    // Confidence bar
    const barY = panelY + 32;
    rect(doc, MARGIN + 6, barY, CONTENT_W - 12, 2, C.border);
    const confW = (CONTENT_W - 12) * ((dataHealth.confidence || 0) / 100);
    const confColor = dataHealth.confidence >= 80 ? C.green : dataHealth.confidence >= 60 ? C.yellow : C.red;
    setFill(doc, confColor);
    doc.rect(MARGIN + 6, barY, confW, 2, 'F');

    writer.y = panelY + 42;
  }

  // Schema table
  if (dataset?.columns?.length) {
    writer.gap(6);
    writer.subLabel('Schema Overview');

    const slicedCols = dataset.columns.slice(0, 12);
    const colW2 = CONTENT_W / 4;
    const rowH2 = 6;
    const headerH2 = 7;

    // Header
    gradientRect(doc, MARGIN, writer.y, CONTENT_W, headerH2);
    doc.setFontSize(7.5);
    doc.setFont('helvetica', 'bold');
    setTxt(doc, C.white);
    ['Column Name', 'Type', 'Missing %', 'Mean / Range'].forEach((h, i) => {
      doc.text(h, MARGIN + colW2 * i + 2, writer.y + 4.5);
    });
    writer.y += headerH2;

    for (let ri = 0; ri < slicedCols.length; ri++) {
      writer.ensureSpace(rowH2 + 2);
      const col = slicedCols[ri];
      const bgC = ri % 2 === 0 ? C.bg : C.surface;
      rect(doc, MARGIN, writer.y, CONTENT_W, rowH2, bgC);
      doc.setFontSize(7.5);
      doc.setFont('helvetica', 'normal');
      setTxt(doc, ri % 2 === 0 ? C.muted : C.white);
      const cells = [
        String(col.name || ''),
        String(col.type || ''),
        col.null_pct != null ? `${col.null_pct.toFixed(1)}%` : '—',
        col.mean != null ? `${col.mean.toFixed(2)} (${col.min}–${col.max})` : '—',
      ];
      cells.forEach((c, ci) => {
        doc.text(c, MARGIN + colW2 * ci + 2, writer.y + rowH2 - 1.5, { maxWidth: colW2 - 3 });
      });
      writer.y += rowH2;
    }
    if (dataset.columns.length > 12) {
      writer.gap(2);
      doc.setFontSize(7.5);
      doc.setFont('helvetica', 'italic');
      setTxt(doc, C.faint);
      doc.text(`… and ${dataset.columns.length - 12} more columns`, MARGIN, writer.y);
      writer.y += 4;
    }
  }

  writer.gap(8);
  writer.hr(C.border);
  // Privacy note
  doc.setFontSize(8);
  doc.setFont('helvetica', 'italic');
  setTxt(doc, C.faint);
  doc.text(
    '🔒 Privacy: Only schema + 5 sample rows were sent to the LLM. Your full dataset never leaves your machine.',
    MARGIN, writer.y,
    { maxWidth: CONTENT_W }
  );
  writer.y += 8;
}

/* ─── Main export function ──────────────────────────────────────────────────── */
async function generateReport({ messages, dataset, mode, dataHealth }) {
  const doc = new jsPDF({ unit: 'mm', format: 'a4', compress: true });
  const writer = new PDFWriter(doc);

  // Pair messages into [user, ai] turns
  const turns = [];
  let i = 0;
  while (i < messages.length) {
    if (messages[i].role === 'user') {
      const user = messages[i];
      const ai   = messages[i + 1]?.role === 'ai' ? messages[i + 1] : null;
      turns.push({ user, ai });
      i += ai ? 2 : 1;
    } else {
      i++;
    }
  }

  const aiCount = turns.filter(t => t.ai && !t.ai.error).length;

  // ── Cover Page ──
  drawCoverPage(writer, dataset, mode, dataHealth, aiCount);

  // ── Q&A Turns ──
  for (let qi = 0; qi < turns.length; qi++) {
    const { user, ai } = turns[qi];

    writer.newPage();

    // Section heading
    writer.sectionHeading(`Q${qi + 1}`, user.text);
    writer.gap(2);

    if (!ai || ai.error) {
      writer.subLabel('Error');
      writer.para(ai?.error || 'No response received.', { color: C.red, size: 9.5 });
      continue;
    }

    // Mode pill
    const modeLabel = ai.mode === 'smart'    ? '⚡ SMART MODE'
                    : ai.mode === 'scalable' ? '🧠 SCALABLE MODE'
                    :                          '⚪ RAW MODE';
    const modeColor = ai.mode === 'smart'    ? C.green
                    : ai.mode === 'scalable' ? C.cyan
                    :                          C.yellow;

    writer.pill(modeLabel, MARGIN, modeColor.map(v => Math.round(v * 0.15)), modeColor);
    writer.y += 3;
    writer.gap(3);

    // SQL
    if (ai.sql?.trim()) {
      writer.subLabel('Generated SQL');
      rect(doc, MARGIN, writer.y - 1, CONTENT_W, Math.min(writer.doc.splitTextToSize(ai.sql, CONTENT_W - 4).length * 4.5 + 6, 35), C.surface);
      writer.doc.setFontSize(8);
      writer.doc.setFont('courier', 'normal');
      setTxt(writer.doc, C.cyan);
      const sqlLines = writer.doc.splitTextToSize(ai.sql, CONTENT_W - 4);
      for (const line of sqlLines.slice(0, 6)) {
        writer.ensureSpace(5);
        writer.doc.text(line, MARGIN + 2, writer.y + 3);
        writer.y += 4;
      }
      writer.y += 4;
    }

    // Explanation
    if (ai.explanation) {
      writer.subLabel('Answer');
      writer.para(ai.explanation, { size: 10, color: C.white });
      writer.gap(2);
    }

    // Insights
    if (ai.insights?.length) {
      writer.subLabel('Key Insights');
      writer.bullets(ai.insights);
    }

    // Why analysis
    if (ai.why_analysis) {
      writer.whyBlock(ai.why_analysis);
    }

    // Chart screenshot
    const chartEl = document.getElementById(`chart-capture-${ai.id}`);
    if (chartEl) {
      writer.subLabel('Chart');
      const imgData = await screenshotElement(chartEl);
      if (imgData) await writer.embedImage(imgData, 72);
    }

    // Result table
    if (ai.result?.length && ai.columns?.length) {
      writer.subLabel('Result Data');
      writer.dataTable(ai.result, ai.columns);
    }

    // Preprocessing log
    if (ai.preprocessing_log?.length) {
      writer.subLabel('Preprocessing Steps');
      writer.preprocLog(ai.preprocessing_log);
    }

    writer.hr();
  }

  // Write footer on every page
  const totalPages = writer.page;
  writer.drawFooter(totalPages, dataset?.filename || 'Report');

  // Save
  const ts = new Date().toISOString().slice(0, 10);
  const name = (dataset?.filename || 'report').replace(/\.csv$/i, '');
  doc.save(`DataLens_${name}_${ts}.pdf`);
}

/* ─── Component ─────────────────────────────────────────────────────────────── */
export default function ReportExporter({ messages, dataset, mode, dataHealth }) {
  const [loading, setLoading] = useState(false);

  const hasContent = messages.some(m => m.role === 'ai' && !m.error);

  const handleExport = useCallback(async () => {
    if (loading || !hasContent) return;
    setLoading(true);
    try {
      await generateReport({ messages, dataset, mode, dataHealth });
    } catch (err) {
      console.error('PDF export failed:', err);
      alert('Could not generate report. Please try again.');
    } finally {
      setLoading(false);
    }
  }, [loading, hasContent, messages, dataset, mode, dataHealth]);

  if (!hasContent) return null;

  return (
    <button
      id="export-report-btn"
      className={`export-report-btn ${loading ? 'loading' : ''}`}
      onClick={handleExport}
      disabled={loading}
      title="Download PDF report of this session"
    >
      {loading ? (
        <>
          <Loader size={14} className="spin-icon" />
          Generating…
        </>
      ) : (
        <>
          <Download size={14} />
          Export Report
        </>
      )}
    </button>
  );
}
