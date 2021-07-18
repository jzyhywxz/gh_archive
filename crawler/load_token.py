import os
import json
import pprint


def load_token(path):
    token = dict()
    if os.path.exists(path):
        with open(path, 'r') as f:
            custom_token: dict = json.load(f)
        for (k, v) in custom_token.items():
            token[k] = v
    return token


def load_mongodb_token(path='./mongodb_token.json'):
    return load_token(path)


def load_gh_archive_token(path='./gh_archive_token.json'):
    return load_token(path)


if __name__ == '__main__':
    print('mongodb token:')
    pprint.pprint(load_mongodb_token())
    print('\ngh_archive token:')
    pprint.pprint(load_gh_archive_token())

