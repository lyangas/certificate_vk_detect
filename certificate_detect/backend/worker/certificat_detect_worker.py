from flask import Flask
from flask import request
import requests
from sqlalchemy.ext.declarative import DeclarativeMeta
import json

from analyze_script import *
from search_tree import SearchTree


from paralleler import ParallelWorker
from db_helper import WorkerDB

'''class :

    def __init__(self):
        self.worker_db = WorkerDB(dbname='cert_detect_queue', password='gtnh1511475', user='root', host='localhost')
        
        
    def get_users(self):
        worker_db.get('users', columns=None, where=None)['data']

    def id_from_queue(self):
        return {'session_id': session_id, 'id': id, 'status': status}'''


search_tree = SearchTree('db/search_tree.json')

def get_users():
    items = User.query.all()
    users = json.loads(json.dumps(items, cls=AlchemyEncoder))
    return users
parallel_worker = ParallelWorker(get_users)


def id_from_queue(): # переделать
    raw_item = 'DELETE queue OUTPUT deleted.* where id = (select min(id) from queue)'

    try:
        item = json.loads(json.dumps(raw_item, cls=AlchemyEncoder))
        session_id = item['id']
        id = item['data']
        status = item['status']

        return {'session_id': session_id, 'id': id, 'status': status}
    except Exception as e:
        return None

def update_status(session_id, new_status): # переделать
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

    id = from_queue['id']
    session_id = from_queue['session_id']
    status = from_queue['status']

    users_ids = []
    for raw_id in ids: # ускорить <===================================================
        time.sleep(0.4)
        try:
            id, id_type = clear_id(raw_id)
        except Exception as e:
            print(str(e))
            continue
            
        if id_type == 'club':
            users_ids += parallel_worker.get_users(id)

        elif id_type == 'user':
            users_ids.append(id)
    print('users_cnt = ', len(users_ids))

    print('start')
    if status != 'in_queue':
        old_cnt = int(float(status.replace('%', '')) * len(users_ids))
    else:
        old_cnt = 0

    requered_users = []
    for index, user_id in enumerate(users_ids): # ускорить <===================================================

        # если данный анализ ранее начинался, но по какой-то причине прервался
        if old_cnt > index:
            continue
        requered_users.append(user_id)

    print('really needed users_cnt = ', len(requered_users))
                
    print('complete analyze!')
    update_status(session_id, 'complete')
    