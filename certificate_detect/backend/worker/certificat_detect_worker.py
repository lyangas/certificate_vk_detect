from flask import Flask
from flask import request
import requests
from sqlalchemy.ext.declarative import DeclarativeMeta
import json

from flask_sqlalchemy import SQLAlchemy

from analyze_script import *
from search_tree import SearchTree
from sqlalchemy import or_, not_, and_

from paralleler import ParallelWorker


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

def get_users():
    items = User.query.all()
    users = json.loads(json.dumps(items, cls=AlchemyEncoder))
    return users
parallel_worker = ParallelWorker(get_users)

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

db.create_all()

def id_from_queue():
    raw_item = Session.query.filter(and_(not_(Session.status.contains('in_queue')), not_(Session.status.contains('complete')))).first()

    try:
        item = json.loads(json.dumps(raw_item, cls=AlchemyEncoder))
        session_id = item['id']
        ids = item['data']
        status = item['status']

        return {'session_id': session_id, 'ids': ids, 'status': status}
    except Exception as e:
        return None

def update_status(session_id, new_status):
    session = Session.query.filter_by(id=session_id).first()
    session.status = new_status
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


def create_cluster(bbs, cluster_name=''):
    """
        создать в БД новый кластер
    """

    cluster = Cluster(bbs=bbs, cluster_name=cluster_name)
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
                                  preview_url = cert['preview_url'],
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

        # добавляем текст в словарь для поиска
        search_tree.update_tree(certificate.bbs, certificate.id, certificate.cluster_id)

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
    trigger_word = cert_data['trigger_word']
    is_hidden = cert_data['is_hidden']
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
        top_cluster_id = create_cluster(bbs=cert_bbs, cluster_name=trigger_word, is_hidden=is_hidden)
    else:
        update_cluster_centroids(bbs=cert_bbs)

    return top_cluster_id


def get_certs_by_cluster(id):
    items = Сertificate.query.filter_by(cluster_id = id).all()
    certs = json.loads(json.dumps(items, cls=AlchemyEncoder))
    return certs

# ==============================================


def clear_id(raw_id):
    # try: int(raw_id)
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
print('==========worker==========')
import time
while True:
    from_queue = id_from_queue()
    if from_queue is None:
        #print('there are not new work')
        time.sleep(5)
        continue
        
    print('new analyze!')

    ids = from_queue['ids']
    session_id = from_queue['session_id']
    status = from_queue['status']

    users_ids = []
    for raw_id in ids: # ускорить <===================================================
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
    print('users_cnt = ', len(users_ids))

    print('start')
    if status != 'in_queue':
        old_cnt = int(float(status.replace('%', '')) * len(users_ids))
    else:
        old_cnt = 0

    for index, user_id in enumerate(users_ids): # ускорить <===================================================

        if old_cnt > index:
            continue
        

        update_status(session_id, '{}%'.format(int(1000*index/len(users_ids))/10))

        print('current user ind: {} from {}.user_id='.format(index+1, len(users_ids), user_id))

        certs_of_user = analyze_user(user_id, target_words=['диплом',
                                                            'сертификат',
                                                            'лицензия',
                                                            'certified',
                                                            'specialist',
                                                            'специалист',
                                                            'эксперт'])

        for cert_data in certs_of_user:
            try:
                cluster_id = clusterize(cert_data)
                cert_data.update({'cluster_id': cluster_id})
                add_certificate(cert_data)

            except Exception as e:
                # если новых данных нет
                print(str(e))
                
    print('complete analyze!')
    update_status(session_id, 'complete')
    