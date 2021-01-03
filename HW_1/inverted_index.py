from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType
from collections import defaultdict
import io
import json
import logging
import sys
import pickle
import codecs

# Входные данные
# Wikipedia (sample):
# ● доступен по ссылке для скачивания - ​ здесь​ ;
# ● предполагается, что файл доступен в режиме read-only в локальной
# директории проекта под названием ​ wikipedia_sample.txt​ ;
# ● формат: текст
# ● в каждой строке:
# ○ article_ID(int) ​ <tab>​ article_name ​ <spaces>​ article_content
# Пример:
# 12
# Anarchism
# Anarchism is often defined as a ...
# inverted index
# {term: set<document_id>}


# Выходной формат “обстрела”
# По результатам “обстрела” stdout должен содержать только ответы на запросы
# (всю остальную вспомогательную информацию пишите в stderr или в логи).
# Ответ на запрос - список идентификаторов документов (статей Википедии),
# разделенных запятыми. Пример:
# ● запрос в файле: “long query”, состоит из двух слов “long” и “query”
# ● допустим в датасете только 3 документа 151, 13, 3998 содержат
# одновременно оба этих слова, тогда ваш ответ: “151,13,3998”. Порядок
# предоставленных документов в ответе не важен (может быть любым). Но
# проверяется, что Вы нашли абсолютно все документы.


DEFAULT_DATASET_PATH = "../resources/wikipedia.sample"
DEFAULT_DUMP_PATH = "resources/inverted.index"

# logger = logging.getLogger(__name__)
logger = logging.getLogger("my_example")


class InvertedIndex:
    def __init__(self, word_to_docs_mapping):
        #self.word_to_docs_mapping = word_to_docs_mapping
        self.word_to_docs_mapping = {
            word: set(doc_ids)
            for word, doc_ids in word_to_docs_mapping.items()
        }

    def query(self, words):
        doc_ids = set()
        if len(words) == 0:
            return doc_ids
        if words[0] in self.word_to_docs_mapping:
            doc_ids = set(self.word_to_docs_mapping[words[0]])
        for word in words[1:]:
            if word in self.word_to_docs_mapping:
                doc_ids = doc_ids.intersection(set(self.word_to_docs_mapping[word]))
            else:
                return set()
            if len(doc_ids) == 0:
                return set()
        return doc_ids



    def dump(self, file_path, storage_policy=None):
        storage_policy = storage_policy or PickleStoragePolicy()   # or JsonStoragePolicy()
        storage_policy.dump(self.word_to_docs_mapping, file_path)

    @classmethod
    def load(cls, file_path, storage_policy=None):
        storage_policy = storage_policy or PickleStoragePolicy()  # or JsonStoragePolicy()
        return cls(storage_policy.load(file_path))


class StoragePolicy:
    def dump(self, word_to_docs_mapping, file_path):
        pass

    def load(self, file_path):
        pass


class PickleStoragePolicy(StoragePolicy):
    def dump(self, word_to_docs_mapping, file_path):
        with open(file_path, 'wb') as write_file:
            pickle.dump(
                {word: list(doc_ids) for word, doc_ids in word_to_docs_mapping.items()},
                write_file, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self, file_path):
        with open(file_path, 'rb') as read_file:
            data = pickle.load(read_file)
        return data


class JsonStoragePolicy(StoragePolicy):
    def dump(self, word_to_docs_mapping, file_path):
        with open(file_path, 'w') as write_file:
            serializable_word_to_docs_mapping = {
                word: list(doc_ids)
                for word, doc_ids in word_to_docs_mapping.items()
            }
            dump = json.dumps(serializable_word_to_docs_mapping)
            write_file.write(dump)

    def load(self, file_path):
        with open(file_path, "r") as read_file:
            data = json.load(read_file)
        return data


class StructStoragePolicy(StoragePolicy):
    def dump(self, word_to_docs_mapping, file_path):
        pass

    def load(self, file_path):
        pass


def load_documents(file_path):
    documents = {}
    with open(file_path) as fin:
        for line in fin:
            line = line.rstrip("\n")
            if line:
                doc_id, content = line.split("\t", 1)
                documents[doc_id] = content
    return documents


def build_inverted_index(documents):
    word_to_docs_mapping = defaultdict(set)
    if len(documents) == 0:
        return InvertedIndex(word_to_docs_mapping)
    else:
        for doc_id, content in documents.items():
            words = content.split()
            for word in words:
                word_to_docs_mapping[word].add(doc_id)
        return InvertedIndex(word_to_docs_mapping)


def setup_parser(parser):

    subparsers = parser.add_subparsers(help="choose command to run")

    # Build parser
    # =========================================================================
    build_parser = subparsers.add_parser(
        "build", help="load dataset, build II and save to file",
        formatter_class=ArgumentDefaultsHelpFormatter,)
    build_parser.add_argument(
        "-d", "--dataset", default=DEFAULT_DATASET_PATH, dest="dataset",
        help="path to dataset to build Inverted Index",)
    build_parser.add_argument(
        "-o", "--output", default=DEFAULT_DUMP_PATH, dest="output",
        help="path to dump of builded Inverted Index",)
    build_parser.set_defaults(callback=process_build_arguments)

    # Query parser
    # =========================================================================
    query_parser = subparsers.add_parser("query", help="query Inverted Index")
    query_group = query_parser.add_mutually_exclusive_group(required=False)
    query_parser.add_argument(
        "-i", "--index", dest="index_file",
        help="path to builded Inverted Index",
        default=DEFAULT_DUMP_PATH, required=False)
    query_group.add_argument(
        "--query-file-cp1251",
        type=str,
        default="",
        dest="query_file_cp1251", help="collection of queries to run against Inverted Index",)
    query_group.add_argument(
        "--query-file-utf8",
        type=str,
        default="",
        dest="query_file_utf8", help="collection of queries to run against Inverted Index",)
    query_parser.set_defaults(
        # dataset=DEFAULT_DATASET_PATH,
        # output=DEFAULT_DUMP_PATH,
        callback=process_query_arguments,)


def process_build_arguments(build_arguments):
    logger.info("start loading documents...")
    documents = load_documents(build_arguments.dataset)
    logger.info("loading documents is complete")
    logger.info("start building inverted index...")
    inverted_index = build_inverted_index(documents)
    logger.info("inverted index is builded")
    inverted_index.dump(build_arguments.output)
    logger.info(f"inverted index dumped to {build_arguments.output}")


def process_query_arguments(query_arguments):
    logger.info(f"start loading inverted index from {query_arguments.index_file}")
    inverted_index = InvertedIndex.load(query_arguments.index_file)
    logger.info("loading inverted index is complete")
    if query_arguments.query_file_utf8:
        read_file = open(query_arguments.query_file_utf8, "r")
        query_lines = read_file.readlines()
        for query_line in query_lines:
            query = query_line.rstrip("\n").split()
            document_ids = inverted_index.query(query)
            print(','.join([str(doc_id) for doc_id in document_ids]))
            logger.debug("the answer to query %s is %s", query, document_ids)
    elif query_arguments.query_file_cp1251:
        with codecs.open(query_arguments.query_file_cp1251, encoding='cp1251', errors='replace') as read_file:
            query_lines = read_file.readlines()
            for query_line in query_lines:
                query = query_line.rstrip("\n").split()
                document_ids = inverted_index.query(query)
                print(','.join([str(doc_id) for doc_id in document_ids]))
                logger.debug("the answer to query %s is %s", query, document_ids)


def setup_logging(arguments):
    logging.basicConfig(
        filename='inverted_index.log',
        level=logging.DEBUG,
        filemode='w',
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",)


def main():
    """It is Python CLI interface for Inverted Index Application
    """
    parser = ArgumentParser(
        prog="inverted-index",
        description="Inverted Index Application: build, query, dump and load",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    setup_parser(parser)
    arguments = parser.parse_args()
    setup_logging(arguments)
    logger.info("Application called with arguments %s", arguments)
    arguments.callback(arguments)

if __name__ == "__main__":
    main()