def generate_xml() -> bytes:

    bucket_name = "newco"
    prefix = ""
    key_count = 10
    delimiter = ""

    xml_header = b"\n".join(
        [
            b'<?xml version="1.0" encoding="UTF-8"?>',
            b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        ]
    )

    xml_bucket_meta = "".join(
        [
            f"<Name>{bucket_name}</Name>",
            f"<Prefix>{prefix}</Prefix>",
            f"<KeyCount>{str(key_count)}</KeyCount>",
            "<MaxKeys>1000</MaxKeys>",
            f"<Delimiter>{delimiter}</Delimiter>",
            "<IsTruncated>false</IsTruncated>",
        ]
    ).encode()

    content_entries = b""
    for key_no in range(key_count):
        content_entries += "".join(
            [
                "<Contents>",
                f"<Key>{str(key_no)}.txt</Key>",
                "<LastModified>2022-01-09T21:03:05.971Z</LastModified>",
                "<ETag>&#34;6f33d51c491de2db7b1a85401e931032&#34;</ETag>",
                "<Size>100</Size>",
                "<Owner>",
                "<ID>random-owner-id</ID>",
                "<DisplayName>ownername</DisplayName>",
                "</Owner>",
                "<StorageClass>STANDARD</StorageClass>",
                "</Contents>",
            ]
        ).encode()

    xml_close = b"</ListBucketResult>"

    return b"".join(
        [
            xml_header,
            xml_bucket_meta,
            content_entries,
            xml_close,
        ]
    )
