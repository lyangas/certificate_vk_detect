import requests
import json
from time import sleep, time

from analyze_script import *
from db_helper import WorkerDB

#print(requests.post('http://0.0.0.0:5080/api/analyze', data=json.dumps({'ids': [10373310], 'session_name': 'new sess'})).text)

with open('config.txt') as f:
    db_ip_port = f.read().split('\n')[0].split(' ')[1].split(':')
    db_ip = db_ip_port[0]
    db_port = int(db_ip_port[1])

worker_db = WorkerDB(dbname='cert_detect_queue', password='gtnh1511475', user='root', host=db_ip, port=db_port)


def work_from_queue():
    try:
        sql_str = "SELECT * FROM queue ORDER BY id LIMIT 1"
        work_data = worker_db.request(sql_str)

        if work_data == []:
            return None

        print(work_data)

        work_data = work_data[0]
        worker_db.delete_rows(table_name='queue', where='id={}'.format(work_data['id']))

        session_id = work_data['session_id']
        work_type = work_data['work_type']
        data = work_data['data']

        return {'session_id': session_id, 'data': data, 'work_type': work_type}
    except Exception as e:
        print(str(e))
        return None

def update_status(session_id, new_status):
    worker_db.update_row(table_name='sessions', id=session_id, datas={'status': json.dumps(new_status)})

def get_status(session_id):
    status = worker_db.get(table_name='sessions', where='id={}'.format(session_id))[0]
    status = json.loads(status['status'])
    print('status ', status)
    return status

def load_clusters():
    """
        загрузить из БД все кластеры (их id и bbs)
    """
    clusters = worker_db.get('clusters')

    #print(clusters[0])

    for cluster_ind in range(len(clusters)):
        clusters[cluster_ind]['bbs'] = json.loads(clusters[cluster_ind]['bbs'])
    return clusters


def create_cluster(bbs, cluster_name):
    """
        создать в БД новый кластер
    """

    try:
        id = worker_db.insert(table_name='clusters', data={'bbs': json.dumps(bbs), 'cluster_name': cluster_name, 'visible_lvl': 0})
        return id

    except Exception as e:
        print('error while creating cluster: ', str(e))
        return None


def add_certificate(cert, session_id=-1):
    """
        добавить сертификат в БД
    """
    try:
        #print(cert)
        # если сертификат уже есть в БД
        cert = worker_db.get(table_name='certificates', where='image_url={}'.format(image_url))[0]
        return None
    except Exception as e:
        # если сертификат не был найден в БД - продолжаем
        pass

    try:
        #print(cert)
        cert_id = worker_db.insert(table_name='certificates', data={'cluster_id': cert['cluster_id'], 
                                                                    'image_url': cert['image_url'],
                                                                    'text_from_image': cert['text_from_image'],
                                                                    'bbs': json.dumps(cert['bbs']),
                                                                    'text_blocks': json.dumps(cert['bbs']),
                                                                    'user_id': cert['user_id'],
                                                                    'post_id': cert['post_id'],
                                                                    'session_id': session_id})
        #print(cert_id)
    except Exception as e:
        print('error while add cert in table certificates: ', str(e))
        return None


    try:
        sess_has_cert_id = worker_db.insert(table_name='session_has_certs', data={'id_session': session_id, 
                                                                                  'id_certificate': cert_id})
    except Exception as e:
        print('error while add cert in table session_has_cert: ', str(e))
        return None

    return cert_id


def update_cluster_centroids(bbs):
    """
        обновить значение "среднего" bbs у кластера
    """
    return 0


def clusterize(cert_data, finded_word):
    cert_bbs = cert_data['bbs'][:20]

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
        top_cluster_id = create_cluster(bbs=cert_bbs, cluster_name=finded_word)
    else:
        update_cluster_centroids(bbs=cert_bbs)

    return top_cluster_id


def get_certs_by_cluster_id(id):
    certs = worker_db.get('certisicates', where='cluster_id={}'.format(id))
    return certs


if __name__ == "__main__":
    while True:
        from_queue = work_from_queue()
        if from_queue is None:
            print('there are not new work')
            sleep(1)
            continue

        print(from_queue)
        user_id = from_queue['data']
        session_id = from_queue['session_id']
        work_type = from_queue['work_type']

        if work_type == 'analyze_user':

            certs_of_user = analyze_user(user_id, target_words=['диплом',
                                                                'сертификат',
                                                                'лицензия',
                                                                'certified',
                                                                'specialist',
                                                                'специалист',
                                                                'эксперт'])

            for cert_data in certs_of_user:
                #try:
                #print(cert_data)
                finded_word = cert_data['finded_word']
                cluster_id = clusterize(cert_data, finded_word)
                cert_data.update({'cluster_id': cluster_id})
                add_certificate(cert=cert_data, session_id=session_id)

                '''except Exception as e:
                    # если новых данных нет
                    print(str(e))'''

            status = get_status(session_id)
            analyzed_cnt = status['analyzed_cnt'] + 1
            all_cnt = status['all_cnt']
            percentage = int(1000 * analyzed_cnt / all_cnt) / 10
            update_status(session_id, {'analyzed_cnt': analyzed_cnt, 'all_cnt': all_cnt, 'str': '{}%'.format(percentage)})

            if status['analyzed_cnt'] == 0:
                with open('start.txt', 'w') as f:
                    f.write(str(int(time())))

            if percentage == 100:
                with open('end.txt', 'w') as f:
                    f.write(str(int(time())))
        