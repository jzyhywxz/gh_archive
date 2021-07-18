import os
import sys
import json
import gzip
import calendar
import gridfs
import traceback
from json.decoder import JSONDecodeError
from pymongo import MongoClient, InsertOne
from pymongo.errors import DocumentTooLarge, WriteError
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from load_token import load_mongodb_token, load_gh_archive_token


class Crawler:
    def __init__(self, gh_archive_dir):
        self.gh_archive_dir = gh_archive_dir
        self.mongodb_token = load_mongodb_token()
        self.gh_archive_token = load_gh_archive_token()
        self.mongodb_client = self.__get_mongodb_client()
        self.db = self.mongodb_client[self.mongodb_token['db']]
    
    def __get_mongodb_client(self) -> MongoClient:
        user = self.mongodb_token['user']
        password = self.mongodb_token['password']
        ip = self.mongodb_token['ip']
        port = self.mongodb_token['port']
        url = f'mongodb://{user}:{password}@{ip}:{port}/?'
        for (k, v) in self.mongodb_token['params'].items():
            url = url + f'{k}={v}&'
        url = url[:-1]
        return MongoClient(url)
    
    def __get_gh_archive_request(self, date, hour) -> Request:
        gh_file_name = f'{date}-{hour}.json.gz'
        url = self.gh_archive_token['url']
        url = f'{url}{gh_file_name}'
        headers = self.gh_archive_token['headers']
        return Request(url, headers=headers)
    
    def __is_hourly_gh_data_exists_in_mongodb(self, date, hour) -> bool:
        col = self.db['status']
        doc = col.find_one({'datetime': f'{date}-{hour}'})
        return doc is not None
    
    def __is_hourly_gh_data_exists_in_localfs(self, date, hour) -> bool:
        gh_file_name = f'{date}-{hour}.json.gz'
        path = os.path.join(self.gh_archive_dir, date, gh_file_name)
        return os.path.exists(path)
    
    def __download_hourly_gh_data_from_server(self, date, hour) -> bool:
        request: Request = self.__get_gh_archive_request(date, hour)
        success = False
        gh_file_name = f'{date}-{hour}.json.gz'
        path = os.path.join(self.gh_archive_dir, date, gh_file_name)
        try:
            response = urlopen(request)
            amt = 1024 * 1024
            gh_file_dir = os.path.join(self.gh_archive_dir, date)
            if not os.path.exists(gh_file_dir):
                os.makedirs(gh_file_dir)
            with open(path, 'wb') as f:
                while True:
                    buffer = response.read(amt)
                    if not buffer:
                        break
                    f.write(buffer)
            success = True
        except HTTPError as e:
            gh_file_name = f'{date}-{hour}.json.gz'
            url = self.gh_archive_token['url']
            url = f'{url}{gh_file_name}'
            sys.stderr.write(f'Failed to get response from {url}, and the error code is {e.code}.\n')
        except URLError as e:
            gh_file_name = f'{date}-{hour}.json.gz'
            url = self.gh_archive_token['url']
            url = f'{url}{gh_file_name}'
            sys.stderr.write(f'Failed to request {url}, and the reason is {e.reason}.\n')
        finally:
            if not success and os.path.exists(path):
                os.remove(path)
        return success
    
    def __get_hourly_gh_data_in_gzip_stream(self, date, hour):
        gh_file_name = f'{date}-{hour}.json.gz'
        path = os.path.join(self.gh_archive_dir, date, gh_file_name)
        if self.__is_hourly_gh_data_exists_in_localfs(date, hour):
            gzip_file = gzip.GzipFile(path)
        else:
            success = self.__download_hourly_gh_data_from_server(date, hour)
            gzip_file = gzip.GzipFile(path) if success else None
        return gzip_file
    
    def __insert_event_by_gridfs(self, date, hour, line):
        fs = gridfs.GridFS(self.db)
        file_id = fs.put(line)
        
        col = self.db['gridfs']
        result = col.insert_one({'file_id': file_id, 'date': date, 'hour': hour})
        assert result.acknowledged
        return file_id
    
    def __delete_event_by_gridfs(self, file_id):
        fs = gridfs.GridFS(self.db)
        fs.delete(file_id)
    
    def __insert_hourly_gh_data_into_mongodb(self, date, hour) -> bool:
        if self.__is_hourly_gh_data_exists_in_mongodb(date, hour):
            sys.stderr.write(f'events generated during {date}-{hour} already exist in the local mongodb.\n')
            return True
        
        gzip_file = self.__get_hourly_gh_data_in_gzip_stream(date, hour)
        if gzip_file is None:
            return False
        
        col = self.db[date]
        is_insertion_complete = True
        inserted_ids = []
        inserted_file_ids = []
        while True:
            try:
                line = gzip_file.readline()
                if not line:
                    break
                event = json.loads(line)
                result = col.insert_one(event)
                if not result.acknowledged:
                    sys.stderr.write(f'Failed to insert the event into the "{date}" collection of the {self.mongodb_token["db"]}.\n')
                    is_insertion_complete = False
                    break
                inserted_ids.append(result.inserted_id)
            except EOFError as e:
                gh_file_name = f'{date}-{hour}.json.gz'
                path = os.path.join(self.gh_archive_dir, date, gh_file_name)
                if os.path.exists(path):
                    os.remove(path)
                sys.stderr.write(f'Compressed file ({path}) ended before the end-of-stream marker was reached.\n')
                is_insertion_complete = False
                break
            except JSONDecodeError as e:
                file_id = self.__insert_event_by_gridfs(date, hour, line)
                inserted_file_ids.append(file_id)
            except DocumentTooLarge as e:
                file_id = self.__insert_event_by_gridfs(date, hour, line)
                inserted_file_ids.append(file_id)
            except WriteError as e:
                file_id = self.__insert_event_by_gridfs(date, hour, line)
                inserted_file_ids.append(file_id)
            except Exception as e:
                sys.stderr.write(f'{traceback.format_exc()}\n')
                is_insertion_complete = False
                break
        
        if is_insertion_complete:
            col = self.db['status']
            result = col.insert_one({'datetime': f'{date}-{hour}'})
            if not result.acknowledged:
                sys.stderr.write(f'Failed to insert the {date}-{hour} document into the "status" collection of the {self.mongodb_token["db"]}.\n')
                is_insertion_complete = False
        
        if not is_insertion_complete:
            if len(inserted_ids) > 0:
                for inserted_id in inserted_ids:
                    result = col.delete_one({"_id": inserted_id})
                    assert result.acknowledged
            if len(inserted_file_ids) > 0:
                for file_id in inserted_file_ids:
                    self.__delete_event_by_gridfs(file_id)
            return False
        return True
    
    def insert_hourly_gh_data_into_mongodb(self, date, hour):
        if self.__insert_hourly_gh_data_into_mongodb(date, hour):
            print(f'pass,{date},{hour}')
        else:
            print(f'fail,{date},{hour}')
    
    def insert_daily_gh_data_into_mongodb(self, date):
        for hour in range(24):
            self.insert_hourly_gh_data_into_mongodb(date, hour)
    
    def insert_monthly_gh_data_into_mongodb(self, year, month):
        _, days = calendar.monthrange(year, month)
        for day in range(1, days + 1, 1):
            date = f'{year}-{month:02d}-{day:02d}'
            self.insert_daily_gh_data_into_mongodb(date)

    def insert_yearly_gh_data_into_mongodb(self, year):
        for month in range(1, 13, 1):
            self.insert_monthly_gh_data_into_mongodb(year, month)


def show_command_tip():
    sys.stderr.write('command: python crawler.py -o [-y] [-m] [-d] [-h]\n')
    sys.stderr.write('  -o: output of the downloads.\n')
    sys.stderr.write('  -y: year.\n')
    sys.stderr.write('  -m: month.\n')
    sys.stderr.write('  -d: day.\n')
    sys.stderr.write('  -h: hour.\n')
    sys.stderr.write('examples:\n')
    sys.stderr.write('  1) get data at 2015-01-01-0: python crawler.y -o /home/user/gh_archive -h 2015-01-01 0;\n')
    sys.stderr.write('  2) get data on 2015-01-01: python crawler.y -o /home/user/gh_archive -d 2015-01-01;\n')
    sys.stderr.write('  3) get data during 2015-01: python crawler.y -o /home/user/gh_archive -m 2015 1;\n')
    sys.stderr.write('  4) get data during 2015: python crawler.y -o /home/user/gh_archive -y 2015.\n')


def main():
    argv = sys.argv
    if len(argv) < 4:
        show_command_tip()
        return
    
    try:
        assert '-o' == argv[1]
        gh_archive_dir = str(argv[2])
        
        if '-y' == argv[3]:
            assert len(argv) == 5
            year = int(argv[4])
            c = Crawler(gh_archive_dir)
            c.insert_yearly_gh_data_into_mongodb(year)
        elif '-m' == argv[3]:
            assert len(argv) == 6
            year, month = int(argv[4]), int(argv[5])
            assert 1 <= month <= 12
            c = Crawler(gh_archive_dir)
            c.insert_monthly_gh_data_into_mongodb(year, month)
        elif '-d' == argv[3]:
            assert len(argv) == 5
            date = str(argv[4])
            c = Crawler(gh_archive_dir)
            c.insert_daily_gh_data_into_mongodb(date)
        elif '-h' == argv[3]:
            assert len(argv) == 6
            date, hour = str(argv[4]), int(argv[5])
            c = Crawler(gh_archive_dir)
            c.insert_hourly_gh_data_into_mongodb(date, hour)
        else:
            show_command_tip()
    except AssertionError as e:
        show_command_tip()


if __name__ == '__main__':
    main()

