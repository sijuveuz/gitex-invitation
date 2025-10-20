import os
import csv
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfMerger
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from openpyxl import Workbook
from django.conf import settings

HEADERS = [
    "Sl. No", "Link Title / Full Name", "Email Address", "Invite Type",
    "Ticket Class", "Link Limit", "Registered", "Expiry Date", "Status"
]

BATCH_SIZE = 5000
THREADS = 4


class BaseExporter:
    def __init__(self, queryset):
        self.queryset = queryset

    def get_data(self):
        for inv in self.queryset.iterator():
            yield [
                inv.guest_name or "",
                inv.guest_email or "",
                inv.source_type or "",
                inv.ticket_type.name if inv.ticket_type else "",
                inv.usage_limit or "",
                inv.usage_count or 0, 
                inv.expire_date or "",
                inv.status or "",
            ]

    def _generate_paths(self, job_id, ext):
        folder = os.path.join(settings.MEDIA_ROOT, "exports")
        os.makedirs(folder, exist_ok=True)
        filename = f"export_{job_id}.{ext}"
        file_path = os.path.join(folder, filename)
        file_url = f"{settings.MEDIA_URL}exports/{filename}"
        return file_path, file_url


class CSVExporter(BaseExporter):
    def export(self, job_id):
        file_path, file_url = self._generate_paths(job_id, "csv")
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            for i, row in enumerate(self.get_data(), start=1):
                writer.writerow([i] + row)
        return file_path, file_url


class ExcelExporter(BaseExporter):
    def export(self, job_id):
        file_path, file_url = self._generate_paths(job_id, "xlsx")
        wb = Workbook()
        ws = wb.active
        ws.append(HEADERS)
        for i, row in enumerate(self.get_data(), start=1):
            ws.append([i] + row)
        wb.save(file_path)
        return file_path, file_url


class PDFExporter(BaseExporter):
    def _render_batch(self, batch_data, batch_index):
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        c = canvas.Canvas(tmp_file.name, pagesize=A4)
        width, height = A4
        left_margin = 20
        top_margin = height - 50
        row_height = 18
        col_widths = [25, 80, 100, 70, 70, 50, 50, 60, 50]
        y = top_margin
        c.setFont("Helvetica", 8)

        # Header
        c.setFillColor(colors.darkblue)
        c.rect(left_margin, y - row_height, sum(col_widths), row_height, fill=1)
        c.setFillColor(colors.white)
        x = left_margin + 2
        for idx, header in enumerate(HEADERS):
            c.drawString(x, y - row_height + 5, header[:20])
            x += col_widths[idx]
        c.setFillColor(colors.black)
        y -= row_height

        # Rows
        for i, row in enumerate(batch_data, start=1):
            if y < 50:
                c.showPage()
                c.setFont("Helvetica", 8)
                y = top_margin

                # Redraw header on new page
                c.setFillColor(colors.darkblue)
                c.rect(left_margin, y - row_height, sum(col_widths), row_height, fill=1)
                c.setFillColor(colors.white)
                x = left_margin + 2
                for idx, header in enumerate(HEADERS):
                    c.drawString(x, y - row_height + 5, header[:20])
                    x += col_widths[idx]
                c.setFillColor(colors.black)
                y -= row_height

            if i % 2 == 0:
                c.setFillColor(colors.whitesmoke)
                c.rect(left_margin, y - row_height, sum(col_widths), row_height, fill=1)
                c.setFillColor(colors.black)

            x = left_margin + 2
            for j, text in enumerate(row):
                c.drawString(x, y - row_height + 5, str(text)[:20])
                x += col_widths[j]
            y -= row_height

        c.save()
        return tmp_file.name

    def export(self, job_id):
        file_path, file_url = self._generate_paths(job_id, "pdf")

        # Prepare data with Sl. No. before batching
        all_data = [[idx + 1] + list(row) for idx, row in enumerate(self.get_data())]
        total = len(all_data)
        batches = [all_data[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

        tmp_files = []
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {
                executor.submit(self._render_batch, batch, idx): idx
                for idx, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                tmp_files.append(future.result())

        # Merge
        merger = PdfMerger()
        for tmp in sorted(tmp_files):
            merger.append(tmp)
        merger.write(file_path)
        merger.close()

        for tmp in tmp_files:
            os.remove(tmp)

        return file_path, file_url
