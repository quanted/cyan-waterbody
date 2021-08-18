import jinja2
from datetime import datetime, timedelta, date
from xhtml2pdf import pisa


def get_env():
    j_env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath), autoescape=select_autoescape())
    return j_env


def generate_report(year:int, day:int, objectids:list=None, conus:bool=False, region:list=None, state:list=None, tribe:list=None, county:list=None):
    j_env = get_env()
    title = "CyANO Waterbody Report"
    html = get_title(year=year, day=day, j_env=j_env, title=title)
    return html


def get_title(year: int, day: int, j_env = None, title: str = None, page_title: str = None):
    if not j_env:
        j_env = get_env()
    report_datetime = date(year=year, month=1, day=1) + timedelta(days=day - 1)
    report_date = report_datetime.strftime("%d %B %Y")
    template = j_env.get_template("report_0_title.html")
    html = template.render(TITLE=title, PAGE_TITLE=page_title, DATE=report_date)
    return html


if __name__ == "__main__":
    year = 2021
    day = 45
    report_html = generate_report(year=year, day=day)
    report_file = open(f"outputs\\test_report_{year}_{day}.pdf", "w+b")
    pisa_status = pisa.CreatePDF(report_html, dest=report_file)
    report_file.close()
