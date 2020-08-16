from flask import Flask, request, jsonify
import requests
from sqlalchemy.ext.declarative import DeclarativeMeta
import json

from flask_sqlalchemy import SQLAlchemy

from analyze_script import *


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

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db/test_certs.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Сertificate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cluster_id = db.Column(db.Integer)
    image_url = db.Column(db.String(255))
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


class Session(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_name = db.Column(db.String(100))
    status = db.Column(db.String(100))
    data = db.Column(db.JSON)


class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String(200))
    vk_id = db.Column(db.String(100))

#db.create_all()

def update_cetrificate_blocks(certificate_id, text_blocks):
    certificate = Certificate.query.filter_by(id=certificate_id).first()
    certificate.text_blocks = text_blocks
    db.session.commit()

def load_clusters():
    """
        загрузить из БД все кластеры (их id и bbs)
    """
    items = Cluster.query.all()  # .order_by(Item.user_id)

    clusters = []
    for cluster in json.loads(json.dumps(items, cls=AlchemyEncoder)):
        bbs = cluster['bbs']
        id = cluster['id']
        clusters.append({'id': id, 'bbs': bbs})

    return clusters


def create_cluster(bbs):
    """
        создать в БД новый кластер
    """

    cluster = Cluster(bbs=bbs)
    try:
        db.session.add(cluster)
        db.session.commit()
        # db.session.close()
    except Exception as e:
        db.session.rollback()
        print(str(e))

    return cluster.id


def add_certificate(cert, session_id=-1):
    """
        добавить сертификат в БД
    """
    certificate = Сertificate.query.filter_by(image_url=cert['image_url']).first()
    if certificate is None:
        certificate = Сertificate(cluster_id=cert['cluster_id'],
                                  image_url=cert['image_url'],
                                  text_from_image=cert['text_from_image'],
                                  bbs=cert['bbs'],
                                  text_blocks=cert['bbs'],
                                  user_id=cert['user_id'],
                                  post_id=cert['post_id'])

        try:
            db.session.add(certificate)
            db.session.commit()
            db.session.rollback()
        except Exception as e:
            print(str(e))

    cert_id = certificate.id
    session_has_cert = Session_has_certs(id_session=session_id, id_certificate=cert_id)
    try:
        db.session.add(session_has_cert)
        db.session.commit()
        # db.session.close()
    except Exception as e:
        db.session.rollback()
        print(str(e))

    return cert_id


def update_cluster_centroids(bbs):
    """
        обновить значение "среднего" bbs у кластера
    """
    return 0


def clusterize(cert_data):
    cert_bbs = cert_data['bbs'][:20]
    # text = cert_data['text_from_image']

    clusters = load_clusters()

    top_cluster_id = None
    top_similar = -1.
    for cluster in clusters:
        cluster_bbs = cluster['bbs'][:20]
        cluster_id = cluster['id']

        similarity = diff_bbs(cluster_bbs, cert_bbs)

        if (similarity < 0.8) and (similarity > top_similar):
            top_similar = similarity
            top_cluster_id = cluster_id

    if top_cluster_id is None:
        top_cluster_id = create_cluster(bbs=cert_bbs)
    else:
        update_cluster_centroids(bbs=cert_bbs)

    return top_cluster_id


def get_certs_by_cluster(id):
    items = Сertificate.query.filter_by(cluster_id = id).all()
    certs = json.loads(json.dumps(items, cls=AlchemyEncoder))
    return certs

# ==============================================
# авторизация


def getUserIDVKByCode(self, code):
    try:
        url = 'https://oauth.vk.com/access_token?client_id=7211908&client_secret=GQfgkrX6XJrHzkhjdONn' \
              '&redirect_uri=http://ml.vtargete.ru:8001/table.html&code=' + code
        res = requests.get(url)
        return res.json()
    except Exception as e:
        print(str(e))


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




@app.route('/api/authVK', methods=['POST'])
def task_authVK():
    code = request.json['code']
    return getUserIDVKByCode(code)


# ==============================================

print('==========api==========')

@app.route('/api/test', methods=['GET'])
def test_res():
    return 'hello world'

import time
@app.route('/api/analyze', methods=['POST'])
def analyze():
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

    return {'response': 'success'}


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

        return {'response': returned_data}
    except Exception as e:
        return {'error': str(e)}


@app.route('/api/get_all_clusters', methods=['GET'])
def get_all_clusters():

    items = Cluster.query.all()
    clusters = json.loads(json.dumps(items, cls=AlchemyEncoder))

    all_clusters = []
    for cluster in clusters:
        cluster_id = cluster['id']
        cluster_name = 'кластер #{}'.format(cluster_id)  # cluster['cluster_name']
        certs = get_certs_by_cluster(cluster_id)

        current_cluster = []
        for cert in certs:
            current_cluster.append({
                'id': cert['id'],
                'text_blocks': cert['text_blocks'],
                'image_url': cert['image_url'],
                'post_id': cert['post_id'],
                'user_id': cert['user_id'],
            })

        all_clusters.append({'cluster_id': cluster_id,
                             'cluster_name': cluster_name,
                             'certificates': current_cluster})

    return {'response': all_clusters}


@app.route('/api/search', methods=['POST'])
def search():
    fuilds = request.json['fuilds']
    target_text = fuilds['text']
    return {'response': []}


@app.route('/api/update_cetrificate_blocks', methods=['POST'])
def update_cetrificate_blocks():
    certificate_id = request.json['certificate_id']
    text_blocks = request.json['text_blocks']

    try:
        update_cetrificate_blocks(certificate_id, text_blocks)
        return {'response': 'success'}
    except Exception as e:
        return {'error': str(e)}


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5080)
