import hashlib

import pytest

from paperforge.processing.normalizer import norm_doi, raw_payload_hash
from paperforge.storage.local import LocalStorage


def test_local_storage_contract(tmp_path):
    st = LocalStorage(tmp_path)
    info = st.write("raw/a/part-1.jsonl", b"x\n")
    assert info.sha256 == hashlib.sha256(b"x\n").hexdigest() == st.checksum("raw/a/part-1.jsonl")
    assert st.exists("raw/a/part-1.jsonl") and st.read("raw/a/part-1.jsonl") == b"x\n"
    assert st.list("raw") == ["raw/a/part-1.jsonl"]
    with pytest.raises(FileExistsError):  # raw is immutable
        st.write("raw/a/part-1.jsonl", b"y")
    assert st.read("raw/a/part-1.jsonl") == b"x\n"
    with pytest.raises(ValueError):
        st.write("../escape", b"z")


@pytest.mark.parametrize("raw,exp", [
    ("10.1000/ABC", "10.1000/abc"), (" https://doi.org/10.1000/ABC ", "10.1000/abc"),
    ("doi:10.1/x", "10.1/x"), ("http://dx.doi.org/10.1/X", "10.1/x"), ("", None), (None, None)])
def test_norm_doi(raw, exp):
    assert norm_doi(raw) == exp


def test_payload_hash_key_order_independent():
    assert raw_payload_hash({"a": 1, "b": [1, 2]}) == raw_payload_hash({"b": [1, 2], "a": 1})
    assert raw_payload_hash({"a": 1}) != raw_payload_hash({"a": 2})


def test_list_snippet_cuts_at_word_boundary_and_keeps_short_text():
    from paperforge.repositories.analytics import _snippet
    assert _snippet(None, 10) is None and _snippet("", 10) is None and _snippet("short text", 50) == "short text"
    out = _snippet("alpha beta gamma delta", 12)
    assert out == "alpha beta…" and len(out) <= 13
