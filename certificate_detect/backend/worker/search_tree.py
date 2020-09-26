import json

class SearchTree:
    def __init__(self, file_name):
        self.file_name = file_name

    def load_tree(self):
        with open(self.file_name) as f:
            return json.loads(f.read())

    def save_tree(self, tree):
        with open(self.file_name, 'w') as f:
            f.write(json.dumps(tree))

    def writeWordToDict(self, word, my_dict, id, cluster_id):
        root = my_dict

        for letter in word.lower():
            my_dict = my_dict.setdefault(letter, {})
            
        value = {"certificate_id": id, "cluster_id": cluster_id}
        if my_dict.get('_end_') is None:
            my_dict['_end_'] = [] 
        if not(value in my_dict['_end_']):
            my_dict['_end_'].append(value)    

        return root

    def prepare(self, clusters):
        tree = {}
        for cluster in clusters:
            certificates = cluster.get('certificates')
            cluster_id = cluster.get('cluster_id')
            for item in certificates:
                id = item.get('id')
                bbs = item.get('bbs')
                for bb in bbs:
                    word = bb.get('text')
                    tree = self.writeWordToDict(word, tree, id, cluster_id)
        
        return tree

    def prepare_bbs(self, bbs, cert_id, cluster_id):
        tree = self.load_tree()
        for bb in bbs:
            word = bb.get('text')
            tree = self.writeWordToDict(word, tree, cert_id, cluster_id)
        return tree

    def search(self, word):
        tree = self.load_tree()
        _end = '_end_'
        current_dict = tree
        for letter in word.lower():
            if letter not in current_dict:
                return []
            current_dict = current_dict[letter]
        
        return self.prepare_answer(current_dict)

    def prepare_answer(self, current_dict):
        if type(current_dict) == list:
            return current_dict
        else:
            certificates_data = []
            for key, val in current_dict.items():
                certificates_data += self.prepare_answer(val) 
            return certificates_data

    def update_tree(self, bbs, cert_id, cluster_id):
        tree = self.prepare_bbs(bbs, cert_id, cluster_id)
        self.save_tree(tree)
