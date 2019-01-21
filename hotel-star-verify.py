import re
import sys
import threading
import requests

global lock


class StarChecker:
    PROVIDER_URL = 'http://trv.jpe2.rpaas.prod.jp.local/provider-domain/providers/search/findByUuid?Uuid='
    providers_404 = []
    problem_map = {}
    total_provider = 0
    process_num = 0

    def __init__(self, lines):
        self.star_map = {}
        for line in lines:
            elements = line.split(',')
            if len(elements) < 2:
                continue
            uuid = elements[0].strip()
            star = elements[1].strip()
            if StarChecker._is_key(uuid):
                self.star_map.setdefault(star, [])
                self.star_map[star].append(uuid)
                StarChecker.total_provider = StarChecker.total_provider + 1
        print('Loaded {0} uuid'.format(StarChecker.total_provider))

    def check_star_rating(self):
        for (star, uuids) in self.star_map.iteritems():
            for uuid in uuids:
                url = StarChecker.PROVIDER_URL + uuid
                response = None
                try:
                    response = requests.get(url)
                    if response.status_code == 404:
                        StarChecker._add_404(uuid)
                        continue
                    resp_json = response.json()
                    StarChecker.verify_star_rating(star, uuid, resp_json)
                    StarChecker._print_progress()
                except Exception as err:
                    print(err)
                    print(response)
                print(star, uuid)
                StarChecker._add_processed_num()

    @classmethod
    def _add_404(cls, uuid):
        lock.acquire()
        cls.providers_404.append(uuid)
        lock.release()

    @classmethod
    def _add_processed_num(cls):
        lock.acquire()
        cls.process_num = cls.process_num + 1
        lock.release()

    @classmethod
    def _add_problem_providers(cls, star, uuid):
        lock.acquire()
        cls.problem_map.setdefault(star, [])
        cls.problem_map[star].append(uuid)
        lock.release()

    @classmethod
    def verify_star_rating(cls, star, uuid, response):
        try:
            if 'status' in response and 404 == response['status']:
                cls._add_404(uuid)
            else:
                if len(response['starRatingTagList']) > 0:
                    star_tags = response['starRatingTagList'][0]['contents']
                    for tag in star_tags:
                        if tag['content'] == star:
                            return True
        except Exception as err:
            print(err)
            print(response)
        cls._add_problem_providers(star, uuid)

    @classmethod
    def _is_key(cls, line):
        return re.match(r'^\S{36}$', line)

    @classmethod
    def _print_progress(cls):
        print('total: {0} processed: {1}'.format(cls.total_provider, cls.process_num),)
        sys.stdout.flush()

    @classmethod
    def write_file(cls, fname):
        if len(cls.problem_map) > 0 or len(cls.providers_404) > 0:
            with open(fname, 'w') as f:
                f.writelines('===missing providers===\n')
                f.writelines('\n'.join(cls.providers_404))
                f.writelines('\n===problem providers===\n')
                for (star, uuids) in cls.problem_map.iteritems():
                    f.writelines('\n=' + star + '=\n')
                    f.writelines('\n'.join(uuids))
                f.write('\n')
        else:
            print('\nno invalid provider')

    @staticmethod
    def split_chunk(l, size):
        for i in range(0, len(l), size):
            yield l[i:i + size]


if __name__ == '__main__':
    lines = [line.strip() for line in open('star_rating.csv')]
    lines = lines[:100]
    chunks = StarChecker.split_chunk(lines, 10)
    threads = []
    lock = threading.Lock()
    for chunk in chunks:
        checker = StarChecker(chunk)
        t = threading.Thread(target=checker.check_star_rating)
        threads.append(t)

    for thread in threads:
        thread.start()
    print('\nbegin processing...\n')

    for thread in threads:
        thread.join()
    print('\nthreads completed\n')

    StarChecker.write_file("result.csv")