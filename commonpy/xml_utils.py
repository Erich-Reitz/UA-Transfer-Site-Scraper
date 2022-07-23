from lxml import etree


def write_etree_element_to_file(element: etree._Element, filename: str):
    with open(filename, "w") as f:
        f.write(etree.tostring(element, pretty_print=True, encoding="utf-8").decode())
    return
