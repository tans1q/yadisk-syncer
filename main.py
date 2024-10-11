import collections
import json
from time import sleep

import requests
from pyairtable.orm import Model, fields as F

token = "<<SET ME>>"
headers = {'Authorization': token}


class Document(Model):
    # MD5 hash of the file
    md5 = F.TextField("md5")
    # MIME type of the file
    mime_type = F.TextField("mime_type")
    # Names of the document, more than one if there are duplicates
    names = F.TextField("names")
    # Yandex public url of the document
    ya_public_url = F.UrlField("ya_public_url")
    # Yandex public key of the document, used to retrieve temporary download link
    ya_public_key = F.TextField("ya_public_key")
    # Yandex resource id of the document. Together with public key it's used to identify the document
    ya_resource_id = F.TextField("ya_resource_id")
    # Flag to indicate if the document was sent for annotation
    sent_for_annotation = F.CheckboxField("sent_for_annotation")
    # Count of pages in the document
    pages_count = F.NumberField("pages_count")

    def __str__(self):
        return self.to_record()['fields'].__str__()

    def __eq__(self, other):
        self_fields = self.to_record()['fields']
        other_fields = other.to_record()['fields']
        self_names_raw = self_fields.pop('names', "[]")
        other_names_raw = other_fields.pop('names', "[]")
        if self_fields != other_fields:
            return False

        self_names = set(json.loads(self_names_raw))
        other_names = set(json.loads(other_names_raw))
        return bool(self_names.intersection(other_names))

    def update(self, other):
        tmp_names = json.loads(self.names) if self.names else []

        fields = other.__dict__
        for key, value in fields.items():
            setattr(self, key, value)

        self.names = json.dumps(
            list(set(tmp_names + json.loads(fields['_fields']['names']))),
            ensure_ascii=False
        )

    def update_names(self, other):
        self.names = json.dumps(
            list(
                set(
                    (json.loads(self.names) if self.names else [])
                    +
                    (json.loads(other.names) if other.names else [])
                )
            ),
            ensure_ascii=False
        )

def main():
    paths = [
        # "/НейроТатарлар/kitaplar/из телеги"
        # "/НейроТатарлар/kitaplar/Ринат"
        # "/НейроТатарлар/kitaplar/Илгиз"
        "/НейроТатарлар/kitaplar/Дима"
    ]
    all_md5s = get_all_md5s()
    for p in paths:
        traverse(p, all_md5s)


def traverse(directory, all_md5s):
    resp = request_metadata(directory, limit=10_000)

    items = resp["_embedded"].get("items", [])

    docs_acc = []
    for fm in [f for f in items if f["type"] == "file"]:
        process_file(fm, all_md5s, docs_acc)
        if len(docs_acc) > 100:
            Document.batch_save(docs_acc)
            docs_acc = []

    Document.batch_save(docs_acc)

    dirs = [f['name'] for f in items if f["type"] == "dir"]
    for dir_name in dirs:
        traverse(f"{directory}/{dir_name}", all_md5s)


not_a_documents = [
        'application/vnd.android.package-archive',
        'image/jpeg',
        'application/x-zip-compressed',
        'application/zip'
        'application/octet',
        'application/octet-stream',
        'text/x-python'
        'application/x-gzip',
        'text-html',
        'application/x-rar',
        'application/x-download',
    ]


def process_file(fm, all_md5s, docs_acc):
    md5 = fm["md5"]
    path = fm["path"]
    print(f"Processing `{md5}` by path `{path}`...")
    mime_type = fm["mime_type"]
    ya_public_key = fm.get("public_key")
    ya_public_url = fm.get("public_url")

    if md5 in all_md5s and mime_type in not_a_documents:
        print(f"Document {md5} has mime type: {mime_type}, unpublishing and removing...")
        if ya_public_key and ya_public_url:
            unpublish(path)
            print(f"Document {md5} is unpublished")
        if doc := find_by_md5(md5):
            doc.delete()
            print(f"Document {md5} is removed")
        all_md5s.remove(md5)
        return

    # get the document's public key and public url from the metadata
    # if the document is not published yet, publish it
    if not (ya_public_key and ya_public_url):
        ya_public_key, ya_public_url = publish_file(fm["path"])

    new_doc_candidate = Document(
        md5=md5,
        mime_type=mime_type,
            names=json.dumps([fm["name"].replace('"', "'").strip()], ensure_ascii=False),
        ya_public_key=ya_public_key,
        ya_public_url=ya_public_url,
        ya_resource_id=fm["resource_id"],
    )

    if md5 not in all_md5s:
        if mime_type in not_a_documents:
            print(f"Document {md5} is new but has mime type: {mime_type}, skipping it...")
            return
        print(f"Document {md5} is new")
        all_md5s.add(md5)
        docs_acc.append(new_doc_candidate)
        return

    old_doc = Document.first(formula=f"md5='{md5}'")
    if old_doc and (old_doc.ya_resource_id != new_doc_candidate.ya_resource_id):
        print(f"Document {fm['path']}` is a duplicate of the document with the same md5 {md5}, removing...")
        remove_file(fm["path"], md5)
        print(f"Document {fm['path']}` is removed")
        old_doc.update_names(new_doc_candidate)
        docs_acc.append(old_doc)
    else:
        print(f"Document {md5} is already exists")


def publish_file(path):
    # publish the file
    resp = requests.put(
        "https://cloud-api.yandex.net/v1/disk/resources/publish",
        headers=headers,
        params={"path": path},
        timeout=30
    )
    resp.raise_for_status()

    # get the public key of the file after publishing
    resp = request_metadata(path)
    ya_public_key = resp.get("public_key")
    if not ya_public_key:
        raise ValueError(f"Public key for `{path}` is not found after publishing the file")
    ya_public_url = resp.get("public_url")
    if not ya_public_url:
        raise ValueError(f"Public url for `{path}` is not found after publishing the file")
    return ya_public_key, ya_public_url


def remove_file(path, md5):
    resp = requests.delete(
        "https://cloud-api.yandex.net/v1/disk/resources",
        headers=headers,
        params={
            "path": path,
            "md5": md5,
        },
        timeout = 30
    )
    resp.raise_for_status()


def request_metadata(path, limit=10_000):
    resp = requests.get(
        "https://cloud-api.yandex.net/v1/disk/resources",
        headers=headers,
        params={
            "path": path,
            "limit": limit,
        },
        timeout = 30
    )
    resp.raise_for_status()
    return resp.json()

def print_unique_mime_types():
    unique_mime_types = set()
    for table in Document:
        for doc in table.all(fields=["mime_type"]):
            unique_mime_types.add(doc.mime_type)
    print(unique_mime_types)

def unpublish(path):
    resp = requests.put(
        "https://cloud-api.yandex.net/v1/disk/resources/unpublish",
        headers=headers,
        params={
            "path": path,
        },
        timeout = 30
    )
    resp.raise_for_status()
    print(resp.json())


def get_all_md5s():
    all_md5s = set()
    for doc in Document.all(fields=["md5"]):
        all_md5s.add(doc.md5)
    return all_md5s


def find_by_md5(md5):
    doc = Document.first(formula=f"md5='{md5}'")
    if doc:
        return doc
    return None


if __name__ == "__main__":
    main()

