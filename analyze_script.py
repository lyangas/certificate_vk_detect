from flask import Flask
from flask import request

import requests

from io import BytesIO
import json

from PIL import Image
import pytesseract
from Levenshtein import distance

import os
import numpy as np
from sklearn.cluster import DBSCAN


def posts_from_wall(ownerId, count, offset):
    access_token = '25527794e79a323559f47c29b1df2c3b6f1eb91d1f818a6c02867d4bf12c57fb7a8e3dc6830bc046ba482'
    version = '5.37'

    filter = "owner"
    extended = '0'

    link = "https://api.vk.com/method/wall.get?access_token=" + access_token + "&owner_id=" + ownerId + "&v=" + version + "&extended=" + extended + "&filter=" + filter + "&count=" + count + "&offset=" + offset
    res = requests.get(link)
    return res.json()


def extract_images_data_from_res(res):
    images_paths = []
    for index, item in enumerate(res['response']['items']):
        try:
            attachments = item['attachments']

            for attachment in attachments:
                versions_of_photo = []
                if attachment['type'] == 'photo':
                    photo = attachment['photo']
                    for key, path in photo.items():
                        if 'photo' in key:
                            versions_of_photo.append({'size': int(key.replace('photo_', '')), 'path': path})

                versions_of_photo = sorted(versions_of_photo, key=lambda k: k['size'], reverse=True)
                best_photo_path = versions_of_photo[0]['path']

                post_url = 'https://vk.com/id{}?w=wall{}_{}'.format(item['from_id'], item['from_id'], item['id'])
                images_paths.append({'photo_url': best_photo_path, 'post_url': post_url})
        except Exception as e:
            pass

    return images_paths


def load_images(images_paths):
    images = []
    for images_data in images_paths:
        image_url = images_data['photo_url']
        post_url = images_data['post_url']

        try:
            res = requests.get(image_url)
            image = Image.open(BytesIO(res.content))
            images.append({'image': image, 'post_url': post_url, 'image_url': image_url})
        except Exception as e:
            pass
    return images


def find_in_text(text, target_words=['сертификат']):
    for target_word in target_words:
        for line in text.split('\n'):
            for word in line.split(' '):
                if len(word) > 5:
                    word = word.lower()
                    dist = distance(word, target_word)
                    if dist < 4:
                        return True
    return False


def get_posts_with_certs(images):
    certs = []
    for image_data in images:
        image = image_data['image']
        post_url = image_data['post_url']
        image_url = image_data['image_url']

        text_from_image = pytesseract.image_to_string(image, lang='rus')
        if find_in_text(text_from_image, target_words=['сертификат']):
            certs.append({'post_url': post_url, 'text_from_image': text_from_image, 'image_url': image_url})
    return certs


def analyze_user(user_id):
    res = posts_from_wall(str(user_id), '30', '0')

    try:

        images_paths = extract_images_data_from_res(res)

        images = load_images(images_paths)

        posts_url_and_text = get_posts_with_certs(images)

    except Exception as e:

        try:
            error_msg = res['error']['error_msg']
            error_code = res['error']['error_code']

        except Exception:
            error_msg = str(e)
            error_code = None

        print('error! user_id: {} msg: {}'.format(user_id, error_msg))
        if error_code == 29:  # если достигли лимита запросов в день
            raise ValueError(error_msg)

        return []

    return posts_url_and_text


def get_members(group_id, offset, count):
    access_token = '25527794e79a323559f47c29b1df2c3b6f1eb91d1f818a6c02867d4bf12c57fb7a8e3dc6830bc046ba482'
    version = '5.37'

    link = "https://api.vk.com/method/groups.getMembers?access_token=" + access_token + "&group_id=" + group_id + "&v=" + version + "&count=" + count + "&offset=" + offset
    res = requests.get(link)
    return res.json()['response']['items']


def get_all_members(group_id):
    members = []
    old_members = [0] * 1000
    i = 0

    while len(old_members) == 1000:
        old_members = get_members(group_id, str(i * 1000), '1000')
        i += 1
        members += old_members

    return members


# ==============================================

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
    return similarity, matrix


def find_longer_dist(matrix):
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
            return matrix[0][index] + max(lens)

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


def get_dist_matrix(users_has_cert):
    dist_matrix = np.zeros((len(users_has_cert), len(users_has_cert)))

    for i in range(len(users_has_cert)):
        for j in range(i, len(users_has_cert)):

            if i == j:
                dist_matrix[i][j] = 0
            else:

                cert_coords1 = users_has_cert[i]['cert_coords'][:10]
                cert_coords2 = users_has_cert[j]['cert_coords'][:10]
                dist5, _ = diff_bbs(cert_coords1, cert_coords2)

                dist_matrix[i][j] = dist5
                dist_matrix[j][i] = dist5
    return dist_matrix


def get_cert_coord(image):
    data = pytesseract.image_to_data(image, lang='rus').split('\n')
    all_rows = [row.split('\t') for row in data]

    columt_names = all_rows[0]
    data_rows = all_rows[1:]

    textes_data = [dict(zip(columt_names, row)) for row in data_rows]

    coords = []
    for text_data in textes_data:
        # if distance(text_data['text'].lower(), 'сертификат') < 4:
        try:
            if len(text_data['text'].replace(' ', '')) > 2:
                img_w = image.size[0]
                img_h = image.size[1]
                coords.append({'text': text_data['text'],
                               'y': int(text_data['top']) / img_h,
                               'x': int(text_data['left']) / img_w,
                               'w': int(text_data['width']) / img_w,
                               'h': int(text_data['height']) / img_h})
        except Exception as e:
            pass
    return coords


def get_users_has_cert(session_name):
    certs_path = 'saved_data/' + session_name + '/'
    users_has_cert = []
    for file_name in os.listdir(certs_path):

        if not '.txt' in file_name:
            continue

        with open(certs_path + file_name) as f:
            user_data = json.loads(f.read())

        user_id = user_data['user_id']
        posts_url_and_text = user_data['posts_url_and_text']

        for post_url_and_text in posts_url_and_text:
            try:
                post_url = post_url_and_text['post_url']
                text_from_image = post_url_and_text['text_from_image']
                image_url = post_url_and_text['image_url']

                res = requests.get(image_url)
                image = Image.open(BytesIO(res.content))
                k = 1  # 256 / max(image.size)
                w = int(image.size[0] * k)
                h = int(image.size[1] * k)

                try:
                    cert_coords = get_cert_coord(image)
                except Exception as e:
                    cert_coords = None
                    print(str(e))

                users_has_cert.append({'user_id': user_id,
                                       'post_url': post_url,
                                       'text_from_image': text_from_image,
                                       'image': image,
                                       'image_url': image_url,
                                       'cert_coords': cert_coords,
                                       'aspect_ratio': w / h})
            except Exception as e:
                print(str(e))
                pass
    return users_has_cert


def analyze(session_name):
    # получаем список пользователей и инфы о сертификатах
    users_has_cert = get_users_has_cert(session_name)

    # находим матрицу расстояний между сертификатами
    dist_matrix = get_dist_matrix(users_has_cert)

    # кластеризуем сертификаты, согласно матрице расстояний
    clustering = DBSCAN(eps=1.2, min_samples=1).fit(dist_matrix)  # 01
    cluster_inds = clustering.labels_

    clusters = {int(cluster_ind): [] for cluster_ind in cluster_inds}

    for cluster_ind, user_has_cert in zip(cluster_inds, users_has_cert):
        clusters[int(cluster_ind)].append({'user_id': user_has_cert['user_id'],
                                           'post_url': user_has_cert['post_url'],
                                           'image_url': user_has_cert['image_url']})

    return clusters