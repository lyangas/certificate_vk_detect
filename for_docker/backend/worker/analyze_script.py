import requests
from io import BytesIO
from PIL import Image
import pytesseract
from Levenshtein import distance
import numpy as np
from time import time, sleep
import json

from db_helper import WorkerDB

#print(requests.post('http://0.0.0.0:5080/api/analyze', data=json.dumps({'ids': [1991323], 'session_name': 'new sess'})).text)

worker_db = WorkerDB(dbname='cert_detect_queue', password='gtnh1511475', user='root', host='localhost') # localhost host.docker.internal

access_token = 'kk'
trying_cnt = 0
LAST_REQ_TIME = time()

def get_token():

    sql_str = "SELECT id, token FROM users ORDER BY time_last_use LIMIT 1"
    users_data = worker_db.request(sql_str)
    print('try to use new token: {}\n'.format(users_data))
    if users_data == []:
        raise ValueError('list of users is empty')

    user_data = users_data[0]
    worker_db.update_row(table_name='users', id=user_data['id'], datas={'time_last_use': int(time())})

    return user_data['token']


def vk_api(method, data):
    global access_token
    global trying_cnt
    global LAST_REQ_TIME

    time_req_delta = max(0, 0.4 - (time() - LAST_REQ_TIME)) # нельзя более 3х запросов в секунду
    sleep(time_req_delta)
    LAST_REQ_TIME = time()

    version = '5.37'

    link = "https://api.vk.com/method/{}?access_token={}&v={}".format(method, access_token, version)
    for key, val in data.items():
        link += '&{}={}'.format(key, str(val))

    res = requests.get(link).json()

    if 'error' in res:
        print('error with vk api: \n{}\n'.format(res))
        if int(res['error']['error_code']) == 29: # лимит запросов в день
            access_token = get_token()
            res = vk_api(method, data)
        elif int(res['error']['error_code']) == 5: # не верный токен
            access_token = get_token()
            res = vk_api(method, data)
        elif int(res['error']['error_code']) == 6: # лимит запросов в секунду
            trying_cnt += 1
            if trying_cnt == 3:
                access_token = get_token()
                trying_cnt = 0
                res = vk_api(method, data)
        else:
            raise ValueError('cant execute vk api')
    return res


# находим id всех пользователей

def get_all_members(group_id):
    group_id = str(group_id)
    if '/' in group_id:
        group_id = group_id.split('/')[-1]

    members = []
    old_members = [0] * 1000
    i = 0

    while len(old_members) == 1000:
        data = {'group_id': group_id,
                'offset': i * 1000,
                'count': 1000}
        old_members = vk_api('groups.getMembers', data)['response']['items']
        i += 1
        members += old_members

    return members


# находим все сертификаты одного пользователя

def download_image(image_url):
    res = requests.get(image_url)
    image = Image.open(BytesIO(res.content))
    return image


def images_from_res(res):
    images_paths = []
    for index, item in enumerate(res):
        try:
            attachments = item['attachments']

            for attachment in attachments:
                versions_of_photo = []
                if attachment['type'] == 'photo':
                    photo = attachment['photo']
                    for key, path in photo.items():
                        if 'photo' in key:
                            versions_of_photo.append({'size': int(key.replace('photo_', '')), 'path': path})

                # находим изображение с самым большим разрешением
                versions_of_photo = sorted(versions_of_photo, key=lambda k: k['size'], reverse=True)
                best_photo_path = versions_of_photo[0]['path']

                # post_url = 'https://vk.com/id{}?w=wall{}_{}'.format(item['from_id'], item['from_id'], item['id'])
                images_paths.append({'post_id': item['id'],
                                     'user_id': item['from_id'],
                                     'image_url': best_photo_path})
        except Exception as e:
            pass

    return images_paths


def find_in_text(text, target_words=['сертификат']):
    for target_word in target_words:
        for line in text.split('\n'):
            for word in line.split(' '):
                if len(word) > 5:
                    word = word.lower()
                    dist = distance(word, target_word)
                    if dist < 4:
                        return target_word
    return None


def get_cert_bbs(image):
    data = pytesseract.image_to_data(image, lang='rus').split('\n')
    all_rows = [row.split('\t') for row in data]

    column_names = all_rows[0]
    data_rows = all_rows[1:]

    texts_data = [dict(zip(column_names, row)) for row in data_rows]

    bbs = []
    for text_data in texts_data:
        try:
            if len(text_data['text'].replace(' ', '')) > 2:
                img_w = image.size[0]
                img_h = image.size[1]
                bbs.append({'text': text_data['text'],
                            'y': int(text_data['top']) / img_h,
                            'x': int(text_data['left']) / img_w,
                            'w': int(text_data['width']) / img_w,
                            'h': int(text_data['height']) / img_h})
        except Exception as e:
            pass

    return bbs


def analyze_user(user_id, count=30, offset=0, target_words=['сертификат']):
    certificates_data = []

    data = {'filter': 'owner',
            'extended': '0',
            'owner_id': user_id,
            'count': count,
            'offset': offset}

    try:
        res_posts = vk_api('wall.get', data)

        images_data = images_from_res(res_posts['response']['items'])

        for image_data in images_data:
            image_url = image_data['image_url']

            image = download_image(image_url)

            text_from_img = pytesseract.image_to_string(image, lang='rus+eng')

            finded_word = find_in_text(text_from_img, target_words=target_words)

            if finded_word is not None:
                bbs = get_cert_bbs(image)

                image_data.update({'text_from_image': text_from_img, 'bbs': bbs, 'finded_word': finded_word})

                certificates_data.append(image_data)

    except Exception as e:

        try:
            error_msg = res_posts['error']['error_msg']
            error_code = res_posts['error']['error_code']

        except Exception:
            error_msg = str(e)
            error_code = None

        print('error! user_id: {} msg: {}'.format(user_id, error_msg))
        if error_code == 29:  # если достигли лимита запросов в день
            raise ValueError(error_msg)

        return []

    return certificates_data





# кластеризуем все изображения и запишем результат в БД

def diff_iou(bb1, bb2):
    """
    bb : dict
        Keys: {'x1', 'x2', 'y1', 'y2'}
        The (x1, y1) position is at the top left corner,
        the (x2, y2) position is at the bottom right corner
    """

    if (bb1 == None) or (bb2 == None):
        return 0.0
    # determine the coordinates of the intersection rectangle
    bb1 = {'x1': bb1['x'], 'x2': bb1['x'] + bb1['w'], 'y1': bb1['y'], 'y2': bb1['y'] + bb1['h'], 'text': bb1['text']}
    bb2 = {'x1': bb2['x'], 'x2': bb2['x'] + bb2['w'], 'y1': bb2['y'], 'y2': bb2['y'] + bb2['h'], 'text': bb2['text']}

    x_left = max(bb1['x1'], bb2['x1'])
    y_top = max(bb1['y1'], bb2['y1'])
    x_right = min(bb1['x2'], bb2['x2'])
    y_bottom = min(bb1['y2'], bb2['y2'])

    if x_right < x_left or y_bottom < y_top:
        return 0.0

    # The intersection of two axis-aligned bounding boxes is always an
    # axis-aligned bounding box
    intersection_area = (x_right - x_left) * (y_bottom - y_top)

    # compute the area of both AABBs
    bb1_area = (bb1['x2'] - bb1['x1']) * (bb1['y2'] - bb1['y1'])
    bb2_area = (bb2['x2'] - bb2['x1']) * (bb2['y2'] - bb2['y1'])

    # compute the intersection over union by taking the intersection
    # area and dividing it by the sum of prediction + ground-truth
    # areas - the interesection area
    iou = intersection_area / float(bb1_area + bb2_area - intersection_area)
    l1 = len(bb1['text'])
    l2 = len(bb2['text'])
    levin_dist = 1 - distance(bb1['text'], bb2['text']) / (l1 + l2)
    iou = iou * levin_dist

    assert iou >= 0.0
    assert iou <= 1.0
    return iou


def find_longer_dist(matrix):
    """
        функция для нахождения пути (в матрице) при котором сумма всех нод данного пути будет максимальна
    """

    # идем горизонтальными полосами
    non_zero_indexes = matrix[0].nonzero()[0]

    if matrix.shape[0] > 1:
        if len(non_zero_indexes) == 0:
            return find_longer_dist(matrix[1:])
        else:
            lens = []
            for index in non_zero_indexes:
                len_through_index = find_longer_dist(matrix[1:])
                lens.append(len_through_index)
            return max(matrix[0]) + max(lens)

    else:
        if len(non_zero_indexes) == 0:
            # если в последнем слое только нули - возвращаем текущую длинну пути
            return 0
        else:
            # ищем максимальную длинну в последнем слое
            lens = []
            for index in non_zero_indexes:
                len_through_index = matrix[0][index]
                lens.append(len_through_index)
            return max(lens)


def diff_bbs(bbs1, bbs2):
    """
        0 - похожи, 1 - не похожи
    """
    matrix = np.zeros((len(bbs1), len(bbs2)))

    for i, bb1 in enumerate(bbs1):
        for j, bb2 in enumerate(bbs2):
            matrix[i][j] = diff_iou(bb1, bb2)

    similarity_rows = 1 - find_longer_dist(matrix) / matrix.shape[0]
    similarity_columnes = 1 - find_longer_dist(matrix) / matrix.shape[0]
    similarity = max(similarity_rows, similarity_columnes)
    return similarity

