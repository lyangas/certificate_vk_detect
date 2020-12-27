import flask
from flask import Flask, request, jsonify
import requests
import json
from collections import defaultdict
from io import BytesIO
from PIL import Image
from time import time

from analyze_script import *
from db_helper import WorkerDB

worker_db = WorkerDB(dbname='cert_detect_queue', password='gtnh1511475', user='root', host='localhost')


def update_blocks(certificate_id, text_blocks):
    worker_db.update_row(table_name='certificates', id=certificate_id, datas={'text_blocks': text_blocks})


def change_visible(cluster_id, visible_lvl):
    worker_db.update_row(table_name='cluster', id=cluster_id, datas={'visible_lvl': visible_lvl})


def add_user_to_db(vk_id, token):
    worker_db.insert(table_name='users', data={'vk_id': vk_id, 'token': token, 'time_last_use': int(time())})


def get_all_users_from_db():
    users = worker_db.get('users')
    return users


def get_certs_by_cluster(cluster_id):
    certs = worker_db.get('users', where='cluster_id={}'.format(cluster_id))
    return certs


def clear_id(raw_id):
    only_info_part = str(raw_id).split('/')[-1]
    if 'club' in only_info_part:
        return only_info_part.replace('club', ''), 'club'
    elif 'id' in only_info_part:
        return only_info_part.replace('id', ''), 'user'
    else:
        id = only_info_part

    try:
        res = vk_api('users.get', {'user_ids': id})
        user_id = res['response'][0]['id']
        return user_id, 'user'
    except Exception as e:
        print(res['error']['error_msg'])

    try:
        res = vk_api('groups.getById', {'group_id': id})
        group_id = res['response'][0]['id']
        return group_id, 'club'
    except Exception as e:
        print(str(e))
        raise ValueError('it is not club or user: {}'.format(raw_id))


def create_session(session_name, data, status):
    id = worker_db.insert(table_name='sessions', data={'session_name': session_name, 'data': json.dumps(data), 'status': json.dumps(status)})
    return id

def find_cert_by_word(word):
    sql_str = "SELECT id FROM certificates WHERE text_from_image like '%{}%'".format(word)
    ids = [row['id'] for row in worker_db.request(sql_str)]
    return ids

def create_works(session_id, work_type, data):
    if work_type == 'analyze_user':
        users_ids = []
        for raw_id in data:
            time.sleep(0.5)
            try:
                id, id_type = clear_id(raw_id)
            except Exception as e:
                print(str(e))
                continue

            if id_type == 'club':
                users_ids += get_all_members(id)

            elif id_type == 'user':
                users_ids.append(id)

        works = []
        for user_id in users_ids:
            works.append({'session_id': session_id, 'work_type': 'analyze_user', 'data': user_id})
        return works

def add_work_in_queue(works):
    for work in works:
        worker_db.insert(table_name='queue', data=work)


white_list = [42200725, 122058319, 374982599]

def getUserIDVKByCode(code, redirect_url):
    try:
        url = 'https://oauth.vk.com/access_token?client_id=7575617&client_secret=rOGYhjPVAIbUj7o1JN1e&redirect_uri=' + redirect_url + '&code=' + code
        res = requests.get(url)
        return res.json()
    except Exception as e:
        print(str(e))

# ==============================================
# авторизация

app = Flask(__name__)

@app.route('/api/authVK', methods=['POST'])
def task_authVK():
    request.get_json(force=True)
    code = request.json['code']
    redirect_url = request.json['redirect_url']
    res = getUserIDVKByCode(code, redirect_url)
    
    try:
        #res = res['response']
        user_data = {'user_vk_id': res['user_id'], 'token': res['access_token']}
        add_user_to_db(user_data['user_vk_id'], user_data['token'])

        if user_data['user_vk_id'] in white_list:
            user_data['access_level'] = 1
        else:
            user_data['access_level'] = 0

        response = flask.Response(json.dumps({'response': user_data}))
        response.headers['Access-Control-Allow-Origin'] = '*'

        return response
        
    except Exception as e:
        print(res)
        return res


@app.route('/api/get_all_users', methods=['GET'])
def get_all_users():
    users = get_all_users_from_db()

    response = flask.Response(json.dumps(users))
    response.headers['Access-Control-Allow-Origin'] = '*'

    return response

# ==============================================

@app.route('/api/test', methods=['GET'])
def test_res():

    response = flask.Response(json.dumps({'response': 'hello world'}))
    response.headers['Access-Control-Allow-Origin'] = '*'

    return response

import time
@app.route('/api/analyze', methods=['POST'])
def analyze():
    request.get_json(force=True)

    '''user_id = request.json['user_id']
    if int(user_id) not in white_list:
        response = flask.Response(json.dumps({'error': 'user has no access for this method. user is not in whitelist'}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response'''

    ids = request.json['ids']
    session_name = request.json['session_name']

    session_id = create_session(session_name=session_name, data=ids, status={'analyzed_cnt': 0, 'all_cnt': len(ids), 'str': 'in_queue'})
    works_data = create_works(session_id=session_id, work_type='analyze_user', data=ids)
    add_work_in_queue(works=works_data)

    response = flask.Response(json.dumps({'response': 'success'}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/api/get_status', methods=['GET'])
def get_status():
    try:
        sessions_data = worker_db.get('sessions')

        returned_data = []
        for session_data in sessions_data:
            returned_data.append({'id': session_data['id'],
                                'name': session_data['session_name'],
                                'status': session_data['status']['str']})

        response = flask.Response(json.dumps({'response': returned_data}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    except Exception as e:
        response = flask.Response(json.dumps({'error': str(e)}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


@app.route('/api/change_cluster_visible', methods=['POST'])
def change_cluster_visible():
    request.get_json(force=True)
    visible_levels = request.json['visible_levels']

    for visible_level in visible_levels:
        try:
            cluster_id = visible_level['cluster_id']
            is_hidden = visible_level['is_hidden']
            change_visible(cluster_id, is_hidden)
        except Exception as e:
            pass

    response = flask.Response(json.dumps({'response': 'success'}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response
    

@app.route('/api/get_all_clusters', methods=['GET'])
def get_all_clusters():

    clusters = worker_db.get('clusters')

    all_clusters = []
    for cluster in clusters:
        cluster_id = cluster['id']
        cluster_name = cluster['cluster_name']
        is_hidden = cluster['is_hidden']
        certs = get_certs_by_cluster(cluster_id)

        current_cluster = []
        for cert in certs:
            current_cluster.append({
                'id': cert['id'],
                'text_blocks': cert['text_blocks'],
                'image_url': cert['image_url'],
                'preview_url': cert['preview_url'],
                'post_id': cert['post_id'],
                'user_id': cert['user_id'],
            })

        all_clusters.append({'cluster_id': cluster_id,
                             'cluster_name': cluster_name,
                             'certificates': current_cluster,
                             'is_hidden': is_hidden})

    clusters_cnt = len(clusters)
    response = flask.Response(json.dumps({'response': {'clusters': all_clusters, 'clusters_cnt': clusters_cnt}}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/api/get_clusters', methods=['POST'])
def get_clusters():
    request.get_json(force=True)
    from_ind = request.json['from']
    to_ind = request.json['to']

    clusters = worker_db.get('clusters')

    all_clusters = []
    for cluster in clusters[from_ind:to_ind]:
        cluster_id = cluster['id']
        cluster_name = cluster['cluster_name']
        is_hidden = cluster['is_hidden']
        certs = get_certs_by_cluster(cluster_id)

        current_cluster = []
        for cert in certs:
            current_cluster.append({
                'id': cert['id'],
                'text_blocks': cert['text_blocks'],
                'image_url': cert['image_url'],
                'preview_url': cert['preview_url'],
                'post_id': cert['post_id'],
                'user_id': cert['user_id'],
            })

        all_clusters.append({'cluster_id': cluster_id,
                             'cluster_name': cluster_name,
                             'certificates': current_cluster,
                             'is_hidden': is_hidden})

    clusters_cnt = len(clusters)
    response = flask.Response(json.dumps({'response': {'clusters': all_clusters, 'clusters_cnt': clusters_cnt}}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/api/search', methods=['POST'])
def search():
    request.get_json(force=True)

    '''user_id = request.json['user_id']
    if user_id not in white_list:
        response = flask.Response(json.dumps({'error': 'user has no access for this method. user is not in whitelist'}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response'''

    from_ind = request.json['from']
    to_ind = request.json['to']

    fuilds = request.json['fuilds']
    required_words = fuilds['required_words']
    any_words = fuilds['any_words']


    any_w_strs = []
    for word in any_words:
        any_w_strs.append("text_from_image like '%{}%'".format(word))
    any_w_str = ' OR '.join(any_w_strs)

    req_w_strs = []
    for word in required_words:
        req_w_strs.append("text_from_image like '%{}%'".format(word))
    req_w_str = ' AND '.join(req_w_strs)
        
    if (len(any_w_str) > 0) and (len(req_w_str) > 0):
        like_str = '({}) AND ({})'.format(any_w_str, req_w_str)
    else:
        like_str = any_w_str + req_w_str

    sql_str = "SELECT id FROM certificates WHERE {}".format(like_str)
    filtred_cert_ids = [row['id'] for row in worker_db.request(sql_str)]

    if len(filtred_cert_ids) == 0:
        response = flask.Response(json.dumps({'response': []}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response

    clusters = defaultdict(list)
    for cert_id in filtred_cert_ids:
        cert = worker_db.get('certificates', where='id={}'.format(cert_id))[0]
        cluster_id = cert['cluster_id']
        clusters[cluster_id].append({
                                    'id': cert['id'],
                                    'text_blocks': cert['text_blocks'],
                                    'image_url': cert['image_url'],
                                    'preview_url': cert['preview_url'],
                                    'post_id': cert['post_id'],
                                    'user_id': cert['user_id']
                                    })
    clusters = dict(clusters)

    all_clusters_data = []
    for cluster_id, certs in list(clusters.items())[from_ind:to_ind]:

        cluster_data = worker_db.get('clusters', where='id={}'.format(cluster_id))[0]

        all_clusters_data.append({'cluster_id': cluster_id,
                             'cluster_name': cluster_data['cluster_name'],
                             'certificates': certs})

    clusters_cnt = len(clusters)
    response = flask.Response(json.dumps({'response': {'clusters': all_clusters_data, 'clusters_cnt': clusters_cnt}}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/api/update_cetrificate_blocks', methods=['POST'])
def update_cetrificate_blocks():
    request.get_json(force=True)

    '''user_id = request.json['user_id']
    if user_id not in white_list:
        response = flask.Response(json.dumps({'error': 'user has no access for this method. user is not in whitelist'}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    '''
    certificate_id = request.json['certificate_id']
    text_blocks = request.json['text_blocks']

    try:
        update_blocks(certificate_id, text_blocks)

        response = flask.Response(json.dumps({'response': 'success'}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    except Exception as e:
        print(str(e))
        response = flask.Response(json.dumps({'error': str(e)}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


@app.route('/preview/<image_id>')
def get_image(image_id):

    print(image_id)
    url = worker_db.get('certificates', where='id={}'.format(image_id))[0]['image_url']
    res = requests.get(url)
    image = Image.open(BytesIO(res.content))
    image = image.resize((100,100))

    img_io = BytesIO()
    image.save(img_io, 'JPEG', quality=70)
    img_io.seek(0)
    return flask.send_file(img_io, mimetype='image/jpeg')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5080)
