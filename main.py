# standard lib
from typing import List, Tuple, Union
import json
from concurrent.futures import ThreadPoolExecutor
import logging
import os

# third party
from lxml import etree
import requests

# this package
from commonpy import json_utils, web_utils, xml_utils
from models import SchoolEquivTable, EquivTableRow

# init logger
logFormat = "%(asctime)s: %(levelname)s: %(message)s"
logging.basicConfig(format=logFormat, level=logging.INFO)
LOGGER = logging.getLogger("main")
LOGGER.addHandler(logging.StreamHandler())

# max number of threads to use.
THREAD_POOL = 20

session = requests.Session()
session.mount(
    "https://",
    requests.adapters.HTTPAdapter(
        pool_maxsize=THREAD_POOL, max_retries=3, pool_block=True
    ),
)

LENGTH_OF_NO_SCHOOL_FOR_CODE = 9344


def write_equiv_table(equiv_table: etree._Element):
    xml_utils.write_etree_element_to_file(equiv_table, "outputs/equiv_table.html")


def get_equiv_table_rows(equiv_table: etree._Element) -> List[etree._Element]:
    all_rows = equiv_table.findall(".//tr")
    return all_rows


def get_cols_in_row(row: etree._Element) -> List[etree._Element]:
    cells = row.findall(".//td")
    return cells


def parse_row(row: etree._Element) -> Union[EquivTableRow, None]:
    columns = get_cols_in_row(row)

    # header row or malformed row
    if len(columns) < 7:
        return

    # get the course designation and number.
    group = columns[0].text
    if group is not None:
        return

    foreign_course_designation = columns[1].text
    foreign_course_number = columns[2].text
    foreign_course_title = columns[4].text

    # get the transfer versions of the course.
    alabama_course_designation = columns[6].text
    alabama_course_number = columns[7].text
    alabama_course_title = columns[9].text
    etr = EquivTableRow(
        foreign_course_designation,
        foreign_course_number,
        foreign_course_title,
        alabama_course_designation,
        alabama_course_number,
        alabama_course_title,
    )
    return etr


def parse_table(equiv_table: etree._Element) -> Tuple[str, List[EquivTableRow]]:
    caption_text = equiv_table.find(".//caption").text
    school_name = caption_text.split("-")[1].strip()

    row_objs = []
    for row in get_equiv_table_rows(equiv_table):
        row_obj = parse_row(row)
        if row_obj is not None:
            row_objs.append(row_obj)

    return school_name, row_objs


def get_equiv_table(response) -> etree._Element:
    parser = etree.HTMLParser()
    tree = etree.fromstring(response.text, parser)
    equiv_table = tree.findall(".//table")[3]
    return equiv_table


def send_request_parse_data(school_code: str) -> Union[SchoolEquivTable, None]:
    response = send_request(school_code)
    if response is None:
        return None
    return parse_data(response, school_code)


def send_request(school_code: str):
    LOGGER.info("Sending request for school code: %s", school_code)
    url = "https://ssb.ua.edu/pls/PROD/rtstreq.P_Inputdata"
    payload = {"search_type": "I", "p_sbgi": school_code}
    response = web_utils.get_response(url, payload, method="POST")

    if len(response.text) == LENGTH_OF_NO_SCHOOL_FOR_CODE:
        return

    return response


def parse_data(response, school_code) -> Union[SchoolEquivTable, None]:
    equiv_table = get_equiv_table(response)
    school_name, row_objects = parse_table(equiv_table)

    return SchoolEquivTable(school_name, school_code, row_objects)


def generate_all_school_codes():
    return [str(i).zfill(6) for i in range(0, 6000)]


def write_data_for_all_schools():
    school_codes = generate_all_school_codes()
    with ThreadPoolExecutor(max_workers=THREAD_POOL) as executor:
        for res in list(executor.map(send_request_parse_data, school_codes)):
            if res is not None:
                with open(f"outputs/{res.school_code}_equiv_table.json", "w") as f:
                    json.dump(res, f, cls=json_utils.EnhancedJSONEncoder)


def print_all_schools_names():
    for school_code in generate_all_school_codes():
        if os.path.exists(f"outputs/{school_code}_equiv_table.json"):
            with open(f"outputs/{school_code}_equiv_table.json", "r") as f:
                school_equiv_table = json.load(f)
                if school_equiv_table is not None:
                    print(school_equiv_table["school_name"])


if __name__ == "__main__":
    write_data_for_all_schools()
    print_all_schools_names()
