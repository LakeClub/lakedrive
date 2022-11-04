import logging

from xml.parsers import expat
from typing import Dict, Union, List, Any

logger = logging.getLogger(__name__)


class XMLParserBucketList:
    """parse (AWS) BucketList XML response"""

    def __init__(self, schema: Dict[str, Union[str, List[Any], Dict[str, Any]]]):
        self.element_parent = ""
        self.element_name = ""
        self.element = ""
        self.tmp_dict: Dict[str, str] = {}

        # initiate results with empty values of expected types
        self.results = schema

    def start_element(self, name: str, _: Any) -> None:
        # create new offspring
        self.element_parent = self.element or ""
        self.element = f"{self.element_parent}.{name}"
        self.element_name = name

        if self.element in self.results:
            current_type = type(self.results[self.element])
        else:
            self.results[self.element] = ""
            return

        if current_type == str:
            return
        # type should be either list or dict (defined in schema)
        assert current_type in [list, dict]

        # reset tmp_dict if a new list object is opened
        if current_type == list:
            self.tmp_dict = {}

    def end_element(self, name: str) -> None:
        # if a list object is closed, append tmp_dict to previous list
        element = self.results[self.element]
        if isinstance(element, list):
            element.append(self.tmp_dict)
        # switch back to parent
        self.element = self.element_parent
        self.element_parent = ".".join(self.element.split(".")[:-1])

    def char_data(self, data: str) -> None:
        # only accept data if not list or dict
        assert isinstance(self.results[self.element], str)
        # parent_type = type(self.results[self.element_parent])
        parent_element = self.results[self.element_parent]
        if type(parent_element) == list:
            tmp_dict = self.tmp_dict
        elif type(parent_element) == dict:
            tmp_dict = parent_element
        else:
            return
        # add value to tmp_dict, append as values can be given in parts (e.g. ETag)
        tmp_dict[self.element_name] = tmp_dict.get(self.element_name, "") + data


def parse_xml(
    body: bytes,
    schema: Dict[str, Union[str, List[Any], Dict[str, Any]]],
) -> Dict[str, Union[str, List[Dict[str, str]], Dict[str, str]]]:

    xmlParser = XMLParserBucketList(schema)
    parser = expat.ParserCreate()

    parser.StartElementHandler = xmlParser.start_element
    parser.EndElementHandler = xmlParser.end_element
    parser.CharacterDataHandler = xmlParser.char_data
    parser.Parse(body)

    return xmlParser.results


def parse_bucket_objects_xml(
    body: bytes,
) -> Dict[str, Union[str, List[Dict[str, str]], Dict[str, str]]]:
    """Parse the xml response of an AWS bucket_list (v2) call,
    pyexpat documentation: https://docs.python.org/3/library/pyexpat.html"""
    # only define dict and list, everything else is a string
    schema: Dict[str, Union[str, List[Any], Dict[str, Any]]] = {
        ".ListBucketResult": {},
        ".ListBucketResult.Contents": [],
        ".ListBucketResult.CommonPrefixes": [],
    }
    return parse_xml(body, schema)


def parse_bucket_buckets_xml(
    body: bytes,
) -> Dict[str, Union[str, List[Dict[str, str]], Dict[str, str]]]:
    schema: Dict[str, Union[str, List[Any], Dict[str, Any]]] = {
        ".ListAllMyBucketsResult.Buckets.Bucket": [],
    }
    return parse_xml(body, schema)
