import flask
from flask import Flask, request, jsonify
import requests
from sqlalchemy.ext.declarative import DeclarativeMeta
import json

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_

from analyze_script import *
from search_tree import SearchTree

from collections import defaultdict

from io import BytesIO
from PIL import Image

class AlchemyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                data = obj.__getattribute__(field)
                try:
                    json.dumps(data)  # this will fail on non-encodable values, like other classes
                    fields[field] = data
                except TypeError:
                    fields[field] = None
            # a json-encodable dict
            return fields

        return json.JSONEncoder.default(self, obj)

search_tree = SearchTree('db/search_tree.json')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db/test_certs.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Сertificate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cluster_id = db.Column(db.Integer)
    image_url = db.Column(db.String(255))
    preview_url = db.Column(db.String(255))
    text_from_image = db.Column(db.Text)
    bbs = db.Column(db.JSON)
    text_blocks = db.Column(db.JSON)
    user_id = db.Column(db.Integer)
    post_id = db.Column(db.Integer)
    session_id = db.Column(db.Integer, default=-1)


class Session_has_certs(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    id_session = db.Column(db.Integer)
    id_certificate = db.Column(db.Integer)
    __table_args__ = (db.UniqueConstraint('id_session', 'id_certificate', name='_session_certificate_uc'),)


class Cluster(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bbs = db.Column(db.JSON)
    cluster_name = db.Column(db.String(100))
    is_hidden = db.Column(db.Boolean, default=False)
    


class Session(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_name = db.Column(db.String(100))
    status = db.Column(db.String(100))
    data = db.Column(db.JSON)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String(200))
    vk_id = db.Column(db.String(100), unique=True)

#db.create_all()

def update_blocks(certificate_id, text_blocks):
    certificate = Сertificate.query.filter_by(id=certificate_id).first()
    certificate.text_blocks = text_blocks
    db.session.commit()

def change_visible(cluster_id, is_hidden):
    cluster = Cluster.query.filter_by(id=cluster_id).first()
    cluster.is_hidden = is_hidden
    db.session.commit()

def add_user_to_db(user_id, token):
    user = User(vk_id=user_id, token=token)
    try:
        db.session.add(user)
        db.session.commit()
        db.session.rollback()
    except Exception as e:
        print(str(e))

def get_all_users_from_db():
    items = User.query.all()
    users = json.loads(json.dumps(items, cls=AlchemyEncoder))
    return users



def get_certs_by_cluster(id):
    items = Сertificate.query.filter_by(cluster_id = id).all()
    certs = json.loads(json.dumps(items, cls=AlchemyEncoder))
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


# ==============================================
# авторизация

white_list = [42200725, 122058319, 374982599]

def getUserIDVKByCode(code, redirect_url):
    try:
        url = 'https://oauth.vk.com/access_token?client_id=7575617&client_secret=rOGYhjPVAIbUj7o1JN1e&redirect_uri=' + redirect_url + '&code=' + code
        res = requests.get(url)
        return res.json()
    except Exception as e:
        print(str(e))


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

print('==========api==========')

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
    session = Session(session_name=session_name,
                        status = 'in_queue',
                        data = ids)

    try:
        db.session.add(session)
        db.session.commit()
        db.session.rollback()
    except Exception as e:
        print(str(e))

    response = flask.Response(json.dumps({'response': 'success'}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/api/get_status', methods=['GET'])
def get_status():
    try:
        items = Session.query.all()
        sessions_data = json.loads(json.dumps(items, cls=AlchemyEncoder))

        returned_data = []
        for session_data in sessions_data:
            returned_data.append({'id': session_data['id'],
                                'name': session_data['session_name'],
                                'status': session_data['status']})

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

    items = Cluster.query.all()
    clusters = json.loads(json.dumps(items, cls=AlchemyEncoder))

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

    clusters_cnt = Cluster.query.count()
    response = flask.Response(json.dumps({'response': {'clusters': all_clusters, 'clusters_cnt': clusters_cnt}}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/api/get_clusters', methods=['POST'])
def get_clusters():
    request.get_json(force=True)
    from_ind = request.json['from']
    to_ind = request.json['to']

    items = Cluster.query.all()
    clusters = json.loads(json.dumps(items, cls=AlchemyEncoder))

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

    clusters_cnt = Cluster.query.count()
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
    required_words = fuilds['required_words'].split(' ')
    any_words = fuilds['any_words'].split(' ')

    if len(required_words) > 0:
        # если есть важные слова - оставляем только сертификаты с ними
        cert_ids = []
        for word in required_words:
            search_result = search_tree.search(word)
            new_cert_ids = [cert_data['certificate_id'] for cert_data in search_result]
            if len(cert_ids) > 0:
                cert_ids = [id for id in new_cert_ids if id in cert_ids]
            else:
                cert_ids = new_cert_ids
    else:
        # иначе - оставляем вообще все сертификаты
        cert_ids = []#list(set([cert_data['certificate_id'] for cert_data in search_tree.search('')]))

    if (len(any_words) > 0) and ((len(cert_ids) > 0) or (len(required_words) == 0)):
        filtred_cert_ids = []
        for word in any_words:
            search_result = search_tree.search(word)
            new_cert_ids = [cert_data['certificate_id'] for cert_data in search_result]
            if len(required_words) > 0:
                filtred_cert_ids += [id for id in new_cert_ids if id in cert_ids]
            else:
                filtred_cert_ids += [id for id in new_cert_ids]
    else:
        filtred_cert_ids = cert_ids

    if len(filtred_cert_ids) == 0:
        response = flask.Response(json.dumps({'response': []}))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response

    #items = Сertificate.query.filter(or_(*[Сertificate.id.like(cert_id) for cert_id in filtred_cert_ids])).all()
    #certs = json.loads(json.dumps(items, cls=AlchemyEncoder))
    certs = []
    cnt_cert_per_op = 200
    for i in range((len(filtred_cert_ids)//cnt_cert_per_op) + 1):
        cert_ids = filtred_cert_ids[i*cnt_cert_per_op: i*cnt_cert_per_op + cnt_cert_per_op]
        items = Сertificate.query.filter(or_(*[Сertificate.id.like(cert_id) for cert_id in cert_ids])).all()
        current_certs = json.loads(json.dumps(items, cls=AlchemyEncoder))
        certs += current_certs

        #item = Сertificate.query.filter_by(id = cert_id).first()
        #cert = json.loads(json.dumps(item, cls=AlchemyEncoder))
        #certs.append(cert)

    clusters = defaultdict(list)
    for cert in certs:
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

        items = Cluster.query.filter_by(id=cluster_id).first()
        cluster_data = json.loads(json.dumps(items, cls=AlchemyEncoder))

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
        #callback = request.args.get('callback')
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
    url = Сertificate.query.filter_by(id=image_id).first().image_url
    res = requests.get(url)
    image = Image.open(BytesIO(res.content))
    image = image.resize((100,100))

    img_io = BytesIO()
    image.save(img_io, 'JPEG', quality=70)
    img_io.seek(0)
    return flask.send_file(img_io, mimetype='image/jpeg')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5080)
