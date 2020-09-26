import time
import requests
import json

import os
from multiprocessing import Process, current_process
import multiprocessing as mp

def get_users():
    return requests.get('http://78.47.176.17:14288/api/get_all_users').json()

def vk_api(method, data, access_token='aa11', old_time=time.time()):

    version = '5.37'

    link = "https://api.vk.com/method/{}?access_token={}&v={}".format(method, access_token, version)
    for key, val in data.items():
        link += '&{}={}'.format(key, str(val))

    res = requests.get(link)

    time_delta = time.time() - old_time
    if time_delta < 0.333:
        time.sleep(0.333 - time_delta)
    return res.json(), time.time()

class paraleller:
    def __init__(self, get_users_fn=get_users):
        self.get_users_fn = get_users_fn


    def get_users(self, user, args, return_dict):
        data = {'group_id': args['group_id'],
                'count': 1000,
                'offset': args['offset']}
        
        try:
            old_time = user['old_time']
            token = user['token']
            res, old_time = vk_api('groups.getMembers', data, token, old_time)
            user['old_time'] = old_time
            
            res = res['response']['items']


            proc_name = current_process().name
            return_dict[proc_name] = res
        except Exception as e:
            print('error!\n', str(e))
            pass

    def get_users(self, group_id):
        users_cnt = vk_api('groups.getById', access_token='', data={'group_id': '163528123', 'fields': 'members_count'})
        users_cnt = users_cnt[0]['response'][0]['members_count']
        self.perform(self.get_users_ones, args={'var': [{'offset': i} for i in range(0, users_cnt, 1000)], 
                                                'static': {'group_id': group_id}})

    def perform(self, fn, args=[]):
        users = get_users_fn()

        for user in users: # брать и записывать в бд
            user['old_time'] = time.time()

        procs = []
        manager = mp.Manager()
        return_dict = manager.dict()
        self.break_var = False
        while len(args['var']) > 0:
            for user in users:
                if len(args) == 0:
                    break
                arg = args['var'].pop()
                arg.update(args['static'])
                proc = Process(target=fn, args=(user, arg, return_dict,))
                procs.append(proc)
                proc.start()
            
            for proc in procs:
                proc.join()

        return [val for arr in return_dict.values() for val in arr]