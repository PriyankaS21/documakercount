from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import resolve1
from pdfminer.pdfparser import PDFParser
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTChar, LTText
import os

# MATCHES BASED ON THE FONT SIZE (I.E., Letter Heading Font Size is 14.010800000000017) & FONT STYLE
# AND BASED ON COMPANY ADDRESS ON THE LETTER HEAD
compaddress1 = "New England Life Insurance Company\nPO Box"
compaddress2 = "Metropolitan Life Insurance Company\nPO Box"
compaddress3 = "Brighthouse Life Insurance Company\nPO Box"
compaddress4 = "Brighthouse Life Insurance Company of NY\nPO Box"

def getheading(element):
    if isinstance(element, LTText):
        for text_line in element:
            for character in text_line:
                if isinstance(character, LTChar) and character.size >= 14 and character.fontname == 'Helvetica-Bold':
                    yield character.get_text()

def getname(pdffile):
    file = open(pdffile, 'rb')
    parser = PDFParser(file)
    document = PDFDocument(parser)
    totalpages = (resolve1(document.catalog['Pages'])['Count'])
    adpagenos = []
    tlpagenos = []
    for pageno, page_layout in enumerate(extract_pages(pdffile)):
        if pageno > 0 and pageno < (totalpages-2):
            for element in page_layout:
                if isinstance(element, LTText):
                    if compaddress1 in element.get_text() or compaddress2 in element.get_text() or compaddress3 in element.get_text() or compaddress4 in element.get_text():
                        adpagenos.append(pageno)
                    head = list(getheading(element))
                    heading = ''.join(head)
                    if len(heading) > 0:
                        tlpagenos.append(pageno)
    common = [pg for pg in adpagenos if pg in tlpagenos]
    return common

total = 0
for root, folder, files in os.walk('downloadpdf'):
    for file in files:
        result = getname(f'{root}\\{file}')
        total += len(result)
        print(f'{file} Contains {len(result)} Letters')
print(f'Total Pages in All PDFs are {total}')