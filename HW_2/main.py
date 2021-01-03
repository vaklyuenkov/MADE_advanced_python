import re
import sys
from collections import defaultdict
from datetime import datetime
import csv
import argparse

import xmltodict as xmltodict

FILE_NAME = 'stackoverflow_posts_sample.xml'
FILE_NAME = 'small.xml'
PATH_STOP_LIST = 'stop_words_en.txt'
PATH_REQUESTS = 'requests.csv'


# 1. Считать все данные, сформировать n словарей по годам
# 2. цикл по requests: суммируем словари из всех необходимых годов
# 3. возвращаем top n (скорее всего слов немного, так что находим max, удаляем его и так n раз)

def setup_parser(parser):
    """
    Defines arguments for parser of arguments
    :param parser: Argparser
    :return: None
    """
    parser.add_argument('--questions', metavar='questions', type=str,
                        help='path to xml wih StackOverflow data')

    parser.add_argument('--stop-words', dest='stop', type=str, default='stop_words_en.txt',
                        help='path to stop waords txt')

    parser.add_argument('--queries', dest='queries', type=str,
                        help='path to queries csv file')


    parser.add_argument('--query-file-cp1251', dest='q_cp1251', type=str,
                        default='',
                        help='path to query in format cp1251')

    parser.add_argument('--query-file-utf8', dest='q_utf', type=str,
                        default='',
                        help='path to query in format cp1251')


def read_queries(file_queries):
    with open(file_queries, 'r') as f:
        my_list = [list(map(int, rec)) for rec in csv.reader(f, delimiter=',')]
    return my_list


def read_stop_words(file_stop: str) -> set:
    with open(file_stop, 'r') as f:
        x = f.read().splitlines()
    return set(x)


def get_count_values_words(list_words: set, score: int, stop_list: set) -> dict:
    wordfreq = dict()
    for word in list_words:
        if word not in stop_list:
            wordfreq[word] = score
    return wordfreq


def get_top_n(merged_dict: dict, top_n: int) -> list:
    # ОБЯЗАТЛЕЬНО подумать что будет, если дадут год, которого нет в датасете
    ans_list = []
    for i in range(top_n):
        # ans = max(merged_dict, key=merged_dict.get)

        max_value = max(merged_dict.values())
        max_keys = [key for key, value in merged_dict.items() if value == max_value]
        max_keys.sort()

        ans_list.append([max_keys[0], max_value])
        del merged_dict[max_keys[0]]
    return ans_list


# 0. Parser
parser = argparse.ArgumentParser(
    prog="inverted-index",
    description="Inverted Index Application: build, query, dump and load",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
setup_parser(parser)
try:
    args = parser.parse_args()
except:
    sys.exit(0)

# 1. Формируем и считываем данные
stop_list = read_stop_words(args.stop)
list_queries = read_queries(args.queries)

cons_dict = defaultdict(dict)
with open(args.questions) as f:
    for line in f:
        root = xmltodict.parse(line, xml_attribs=True, attr_prefix='')
        if root['row']['PostTypeId'] != '1':
            continue

        year = datetime.strptime(root['row']['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f').year

        dict_count_values = get_count_values_words(set(re.findall("\w+", root['row']['Title'].lower())),
                                                   int(root['row']['Score']), stop_list)

        cons_dict[year] = {k: cons_dict[year].get(k, 0) + dict_count_values.get(k, 0) for k in
                           set(cons_dict[year]) | set(dict_count_values)}

# 2. Цикл по всем реквестам
for (init_year, final_year, top_n) in list_queries:
    merged_dict = cons_dict[init_year].copy()
    for i in range(init_year + 1, final_year + 1):
        merged_dict = {k: merged_dict.get(k, 0) + cons_dict[i].get(k, 0) for k in
                       set(merged_dict) | set(cons_dict[i])}

    # 3. Получаем список наиболее частых слов
    top_n_list = get_top_n(merged_dict, top_n)
    ans_dict = {'start': init_year, 'end': final_year, "top": top_n_list}
    print(ans_dict)
