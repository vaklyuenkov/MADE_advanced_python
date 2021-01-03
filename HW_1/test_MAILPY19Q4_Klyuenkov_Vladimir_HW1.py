import pytest
import logging
import os
from argparse import Namespace
from inverted_index import InvertedIndex, load_documents, build_inverted_index, JsonStoragePolicy, PickleStoragePolicy, process_build_arguments#, ArrayStoragePolicy

#DATASET_BIG_FPATH = "resources/wikipedia.sample"
DATASET_SMALL_FPATH = "resources/small_wikipedia.sample"
INDEX_TMP_PATH = "resources/tmp.index"
TEST_QUERY_PATH = "resources/test_query_1"

from inverted_index import process_query_arguments


def test_init_empty_inverted_index_do_not_raise_exception():
    word_to_docs_mapping = {}
    inverted_index = InvertedIndex(word_to_docs_mapping)

    storage_policy = JsonStoragePolicy()
    inverted_index.dump(INDEX_TMP_PATH, storage_policy=storage_policy)
    loaded_inverted_index = inverted_index.load(INDEX_TMP_PATH, storage_policy=storage_policy)
    assert word_to_docs_mapping == word_to_docs_mapping

    storage_policy = PickleStoragePolicy()
    inverted_index.dump(INDEX_TMP_PATH, storage_policy=storage_policy)
    loaded_inverted_index = inverted_index.load(INDEX_TMP_PATH, storage_policy=storage_policy)
    assert word_to_docs_mapping == word_to_docs_mapping


# Test build
# =====================================================================================


def test_build_inverted_index_do_not_raise_exception():
    documents = []
    build_inverted_index(documents)


@pytest.fixture
def get_inverted_index():
    documents = load_documents(DATASET_SMALL_FPATH)
    inverted_index = build_inverted_index(documents)
    return inverted_index


def remove_tmp_index():
    if os.path.exists(INDEX_TMP_PATH):
        os.remove(INDEX_TMP_PATH)


def test_can_dump_and_load_inverted_index_json_policy(get_inverted_index):
    inverted_index = get_inverted_index
    storage_policy = JsonStoragePolicy()
    inverted_index.dump(INDEX_TMP_PATH, storage_policy=storage_policy)
    loaded_inverted_index = inverted_index.load(INDEX_TMP_PATH, storage_policy=storage_policy)
    assert inverted_index.word_to_docs_mapping == loaded_inverted_index.word_to_docs_mapping, (
        "dumped and loaded version of inverted_index is different from in-memory one")
    remove_tmp_index()



def test_can_dump_and_load_inverted_index_pickle_policy(get_inverted_index):
    inverted_index = get_inverted_index
    storage_policy = PickleStoragePolicy()
    inverted_index.dump(INDEX_TMP_PATH, storage_policy=storage_policy)
    loaded_inverted_index = inverted_index.load(INDEX_TMP_PATH, storage_policy=storage_policy)
    assert inverted_index.word_to_docs_mapping == loaded_inverted_index.word_to_docs_mapping, (
        "dumped and loaded version of inverted_index is different from in-memory one")
    remove_tmp_index()


@pytest.fixture
def test_index_dump():
    query_arguments = Namespace(
        dataset=DATASET_SMALL_FPATH,
        output=INDEX_TMP_PATH
    )
    process_build_arguments(query_arguments)


def test_index_dump_and_load_equal(get_inverted_index):

    query_arguments = Namespace(
        dataset=DATASET_SMALL_FPATH,
        output=INDEX_TMP_PATH
    )
    process_build_arguments(query_arguments)  # dump index builded from DATASET_SMALL_FPATH

    inverted_index = get_inverted_index
    storage_policy = PickleStoragePolicy()
    loaded_inverted_index = inverted_index.load(INDEX_TMP_PATH, storage_policy=storage_policy)
    assert inverted_index.word_to_docs_mapping == loaded_inverted_index.word_to_docs_mapping, (
        "dumped and loaded version of inverted_index is different from in-memory one")
    remove_tmp_index()


def test_biuld_bad_dataset_way(get_inverted_index):
    try:
        get_inverted_index
        query_arguments = Namespace(
            dataset="bad_way",
            output=INDEX_TMP_PATH)
        process_build_arguments(query_arguments)
    except FileNotFoundError:
        remove_tmp_index()
    else:
        raise Exception('Test fails!')


# Test query
# =====================================================================================


def test_query_utf(capsys, test_index_dump):
    test_index_dump
    query_arguments = Namespace(
        index_file=INDEX_TMP_PATH,
        query_file_utf8=TEST_QUERY_PATH,
    )
    process_query_arguments(query_arguments)
    captured = capsys.readouterr()
    assert set(["12", "290", "25"]) == set(captured.out.replace('\n', '').split(","))
    remove_tmp_index()


def test_bad_query_file(test_index_dump):
    test_index_dump
    try:
        query_arguments = Namespace(
            index_file=INDEX_TMP_PATH,
            query_file_utf8="no_file",
        )
        process_query_arguments(query_arguments)

    except FileNotFoundError:
        remove_tmp_index()
    else:
        raise Exception('Test fails!')


@pytest.mark.xfail(raises=FileNotFoundError)
def test_bad_index_file(test_index_dump):
    query_arguments = Namespace(
        index_file="wrong_index",
        query_file_utf8=TEST_QUERY_PATH,
    )
    process_query_arguments(query_arguments)

