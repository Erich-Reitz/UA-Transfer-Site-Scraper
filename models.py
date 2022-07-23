from dataclasses import dataclass
from typing import List

@dataclass
class EquivTableRow:
    foreign_course_designation: str
    foreign_course_number: str
    foreign_course_title: str

    alabama_course_designation: str
    alabama_course_number: str 
    alabama_course_title: str


@dataclass
class SchoolEquivTable:
    school_name: str
    school_code: str
    rows: List[EquivTableRow]