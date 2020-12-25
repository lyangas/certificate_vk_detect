import flask
from flask import Flask, request

import requests
from io import BytesIO
from PIL import Image
import pytesseract
from Levenshtein import distance
import numpy as np
from time import sleep


text_height_threshold = 0.032


def vk_api(method, data):
    access_token = 'f54fe3b72906d9d9194ad529c1cd65d8f20e0bee7086f93a8d01ebea4ca66940af1eca4bcf21887f02a66'
    version = '5.37'

    link = "https://api.vk.com/method/{}?access_token={}&v={}".format(method, access_token, version)
    for key, val in data.items():
        link += '&{}={}'.format(key, str(val))

    res = requests.get(link)
    return res.json()


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

        try:
            old_members = vk_api('groups.getMembers', data)
            old_members = old_members['response']['items']
        except Exception as e:
            if old_members['error']['error_code'] == 15:
                break
            else:
                print('error: old_members = ', old_members)
                raise ValueError(str(e))
        i += 1
        members += old_members
        sleep(0.5)

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
                smallest_photo_path = versions_of_photo[-1]['path']

                # post_url = 'https://vk.com/id{}?w=wall{}_{}'.format(item['from_id'], item['from_id'], item['id'])
                images_paths.append({'post_id': item['id'],
                                     'user_id': item['from_id'],
                                     'image_url': best_photo_path,
                                     'preview_url': smallest_photo_path})
        except Exception as e:
            pass

    return images_paths


def find_in_text(text, target_words=['сертификат']):
    for target_word in target_words:
        for line in text.split('\n'):
            for word in line.split(' '):
                if len(word) > 5:
                    word = word.lower()
                    if target_word in word:
                        return True, target_word

                    dist = distance(word, target_word)
                    if dist < 2:
                        return True, target_word
    return False, ''


def get_cert_bbs(image):
    data = pytesseract.image_to_data(image, lang='rus').split('\n')
    all_rows = [row.split('\t') for row in data]

    column_names = all_rows[0]
    data_rows = all_rows[1:]

    texts_data = [dict(zip(column_names, row)) for row in data_rows]

    bbs = []
    max_text_height = 0
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
                if (int(text_data['height']) / img_h) > max_text_height:
                    max_text_height = int(text_data['height']) / img_h
        except Exception as e:
            pass

    return bbs, max_text_height


def analyze_user(user_id, count=30, offset=0, target_words=['сертификат']):
    certificates_data = []

    data = {'filter': 'owner',
            'extended': '0',
            'owner_id': user_id,
            'count': count,
            'offset': offset}
    res_posts = vk_api('wall.get', data)

    try:
        images_data = images_from_res(res_posts['response']['items'])

        for image_data in images_data:
            image_url = image_data['image_url']

            image = download_image(image_url)

            text_from_img = pytesseract.image_to_string(image, lang='rus+eng')

            is_finded, trigger_word = find_in_text(text_from_img, target_words=target_words)
            if is_finded:
                bbs, max_text_height = get_cert_bbs(image)

                if max_text_height < text_height_threshold:
                    is_hidden = True
                else:
                    is_hidden = False

                image_data.update({'text_from_image': text_from_img, 'bbs': bbs, 'trigger_word': trigger_word, 'is_hidden': is_hidden})

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


app = Flask(__name__)

@app.route('/analyze', methods=['POST'])
def update_cetrificate_blocks():
    print('start analyze')
    user_id = request.json['user_id']

    certs_of_user = analyze_user(user_id, target_words=['диплом',
                                                        'сертификат',
                                                        'лицензия',
                                                        'certified',
                                                        'specialist',
                                                        'специалист',
                                                        'эксперт'])

    return {'response': certs_of_user}

@app.route('/', methods=['GET'])
def test():

    return {'test': 'aaa'}

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5050)