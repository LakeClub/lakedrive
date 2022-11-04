from lakedrive.s3.xmlparse import parse_bucket_objects_xml

from ..helpers.generate_s3_list import generate_xml


def test_parse_bucket_list_xml() -> None:
    xml_content = generate_xml()
    parsed_contents = parse_bucket_objects_xml(xml_content)

    results_meta = parsed_contents[".ListBucketResult"]
    file_contents = parsed_contents[".ListBucketResult.Contents"]
    prefix_contents = parsed_contents[".ListBucketResult.CommonPrefixes"]

    assert isinstance(results_meta, dict)
    assert isinstance(file_contents, list)
    assert isinstance(prefix_contents, list)
