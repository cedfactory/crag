import io
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

#
# reportlab
#
from reportlab.pdfgen import canvas
from reportlab.lib import utils,colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
from reportlab.platypus import SimpleDocTemplate,Spacer,Paragraph,Table,TableStyle,Image,PageBreak
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle


def get_image(path, width=70 * cm):
    img = utils.ImageReader(path)
    iw, ih = img.getSize()
    aspect = ih / float(iw)
    return Image(path, width=width, height=(width * aspect))


class FooterCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []
        self.width, self.height = A4

    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        page_count = len(self.pages)
        for page in self.pages:
            self.__dict__.update(page)
            if (self._pageNumber > 1):
                self.draw_canvas(page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_canvas(self, page_count):
        page = "Page %s / %s" % (self._pageNumber, page_count)
        self.saveState()
        self.setStrokeColorRGB(0, 0, 0)
        self.setLineWidth(0.5)
        self.line(66, 78, A4[0] - 66, 78)
        self.setFont('Times-Roman', 10)
        self.drawString(A4[0] - 128, 65, page)
        self.restoreState()


class PdfDocument():
    def __init__(self, title, logo):
        self.title = title
        self.logo = logo
        self.elements = []

        # First page
        spacer = Spacer(30, 200)
        self.elements.append(spacer)

        img = get_image(self.logo, 4 * cm)
        self.elements.append(img)

        spacer = Spacer(30, 10)
        self.elements.append(spacer)

        style_title = ParagraphStyle(name='title', fontSize=44, alignment=TA_CENTER)
        self.elements.append(Paragraph(self.title, style_title))

        spacer = Spacer(10, 350)
        self.elements.append(spacer)

        style_subtitle = ParagraphStyle('subtitle', fontSize=9, leading=14, justifyBreaks=1, alignment=TA_RIGHT,
                                        justifyLastLine=1)
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        text = "Generated : {}<br/>".format(current_time)
        paragraphReportSummary = Paragraph(text, style_subtitle)
        self.elements.append(paragraphReportSummary)

        self.elements.append(PageBreak())

    def add_page(self, title, contents):
        styles = getSampleStyleSheet()
        style_page_title = ParagraphStyle(name='page_title', parent=styles['Heading1'], alignment=TA_LEFT)

        self.elements.append(Paragraph(title, style_page_title))
        self.elements.append(Spacer(10, 22))

        for content in contents:
            if isinstance(content, pd.DataFrame):
                df = content
                df = df.round(decimals=3)  # keep 3 decimals
                #df = df.reset_index()  # reset the index to consider it as a column
                table_content = [df.columns[:, ].values.astype(str).tolist()] + df.values.tolist()
                table = Table(table_content, rowHeights=[8] * (len(df) + 1))
                table_content_style = TableStyle([('FONTSIZE', (0, 0), (-1, -1), 4),
                                                  ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                                                  ('TEXTCOLOR', (0, 0), (-1, 0), colors.red),
                                                  ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
                                                  ('BOX', (0, 0), (-1, -1), 1, colors.black)])
                table.setStyle(table_content_style)

                self.elements.append(table)

            elif isinstance(content, plt.Figure):
                fig = content
                imgBuffer = io.BytesIO()
                fig.savefig(imgBuffer, format='png')
                imgBuffer.seek(0)

                self.elements.append(Image(imgBuffer))

            else:
                img = get_image(content, 11 * cm)
                self.elements.append(img)

        self.elements.append(PageBreak())

    def onMyLaterPages(self, canvas, doc):
        canvas.saveState()
        canvas.drawImage(self.logo, A4[0] - 600, A4[1] - 50, width=100, height=20, preserveAspectRatio=True)
        canvas.line(30, 788, A4[0] - 50, 788)

    def save(self, filename):
        doc = SimpleDocTemplate(filename, pagesize=A4)
        doc.build(self.elements, canvasmaker=FooterCanvas, onLaterPages=self.onMyLaterPages)

