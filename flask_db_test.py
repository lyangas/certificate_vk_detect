from flask_sqlalchemy import SQLAlchemy
from flask import Flask

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test_certs.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Item(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cluster_id = db.Column(db.Integer)
    text_from_img = db.Column(db.Text)
    user_id = db.Column(db.Integer)
    post_id = db.Column(db.Integer)


@app.route('/')
def get_item():
    print(111)
    item = Item.query.order_by(Item.user_id).all()
    print(item)
    print(item[0].text_from_img)
    return {'res': 0}


@app.route('/create')
def create_item():
    item = Item(cluster_id=0, text_from_img='это текст', user_id=123, post_id=11)
    try:
        db.session.add(item)
        db.session.commit()
    except Exception as e:
        print(str(e))

    return {'res': 0}


# from flask_db_test import db
# db.create_all()

if __name__ == "__main__":
    app.run(host='0.0.0.0')
