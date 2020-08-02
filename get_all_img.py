import requests

from io import BytesIO
import json

from PIL import Image

import os

def posts_from_wall (ownerId, count, offset):
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

                post_url = 'wall{}_{}'.format(item['from_id'], item['id'])
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
        
            
def analyze_user(user_id):
    res = posts_from_wall(str(user_id), '30', '0')

    try:
        
        images_paths = extract_images_data_from_res(res)

        images = load_images(images_paths)

        #создать директорию если ее нет
        user_folder = 'users/{}/'.format(user_id)
        if not os.path.exists(user_folder):
            os.makedirs(user_folder)
            
        for image in images:
            image['image'].save(user_folder + image['post_url'] + '.png')
        
    except Exception as e:
        
        try:
            error_msg = res['error']['error_msg']
            error_code = res['error']['error_code']
            
        except Exception:
            error_msg = str(e)
            error_code = None
            
        print('error! user_id: {} msg: {}'.format(user_id, error_msg))
        if error_code == 29: # если достигли лимита запросов в день
            raise ValueError(error_msg)
            
        return []
    
    return 'success'
    

def get_members (group_id, offset, count):
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
        old_members = get_members (group_id, str(i*1000), '1000')
        i += 1
        members += old_members
        
    return members

#==============================================
    

#==============================================

group_id = 'pumptraffic'

user_ids = get_all_members(group_id)

for index, user_id in enumerate(user_ids):
    if index % 10 == 0:
        print('current user {} / {}'. format(index, len(user_ids)))

    try:
        posts_url_and_text = analyze_user(user_id)
    except Exception as e: # ошибка если достигли лимита запросов в день
        break