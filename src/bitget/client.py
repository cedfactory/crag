import requests
import json
from . import consts as c, utils, exceptions
import time

class Client(object):

    def __init__(self, api_key, api_secret_key, passphrase, use_server_time=False, first=False):

        self.API_KEY = api_key
        self.API_SECRET_KEY = api_secret_key
        self.PASSPHRASE = passphrase
        self.use_server_time = use_server_time
        self.first = first

    def _request(self, method, request_path, params, cursor=False):
        if method == c.GET:
            request_path = request_path + utils.parse_params_to_str(params)
        # url
        url = c.API_URL + request_path

        # Get local time
        timestamp = utils.get_timestamp()


        # sign & header
        if self.use_server_time:
            # Get server time interface
            timestamp = self._get_timestamp()

        body = json.dumps(params) if method == c.POST else ""
        sign = utils.sign(utils.pre_hash(timestamp, method, request_path, str(body)), self.API_SECRET_KEY)
        header = utils.get_header(self.API_KEY, sign, timestamp, self.PASSPHRASE)

        if self.first:
            print("url:", url)
            print("method:", method)
            print("body:", body)
            print("headers:", header)
            # print("sign:", sign)
            self.first = False

        # start = time.time()

        # send request
        response = None
        if method == c.GET:
            # response = requests.get(url, headers=header)
            with requests.get(url, headers=header) as response:
                pass
        elif method == c.POST:
            # response = requests.post(url, data=body, headers=header)
            with requests.post(url, data=body, headers=header) as response:
                pass
        elif method == c.DELETE:
            # response = requests.delete(url, headers=header)
            with requests.delete(url, headers=header) as response:
                pass

        response.close()

        # end = time.time()
        # print("elapsed_time: ", response.elapsed, "   -   requests: {} {}".format(end - start, url))

        # exception handle
        if not str(response.status_code).startswith('2'):
            print("url : ", url)
            print("response : ", response)
            if hasattr(response, "content"):
                content = response.content.decode('utf-8')
                dict_content = json.loads(content)
                code = dict_content.get("code", "no code")
                msg = dict_content.get("msg", "no msg")
                print("code : ", code)
                print("msg : ", msg)
            raise exceptions.BitgetAPIException(response)
        try:
            res_header = response.headers
            if cursor:
                r = dict()
                try:
                    r['before'] = res_header['BEFORE']
                    r['after'] = res_header['AFTER']
                except:
                    pass
                locals().clear()
                return response.json(), r
            else:
                del body
                del header
                del method
                del params
                del request_path
                del sign
                del timestamp
                del url
                object_json = response.json()
                del response
                locals().clear()
                return object_json

        except ValueError:
            raise exceptions.BitgetRequestException('Invalid Response: %s' % response.text)

    def _request_without_params(self, method, request_path):
        return self._request(method, request_path, {})

    def _request_with_params(self, method, request_path, params, cursor=False):
        return self._request(method, request_path, params, cursor)

    def _get_timestamp(self):
        url = c.API_URL + c.SERVER_TIMESTAMP_URL
        response = requests.get(url)
        response.close()
        json_object_data = response.json()['data']
        if response.status_code == 200:
            del response
            return json_object_data
        else:
            del response
            return ""
