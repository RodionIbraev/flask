from datetime import datetime

import pydantic as pydantic
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.orm import sessionmaker
from flask import Flask, jsonify, request
from flask.views import MethodView

app = Flask('app')

BaseModel = declarative_base()

PG_DSN = 'postgresql://admin:1234@127.0.0.1/flask_task'
engine = create_engine(PG_DSN)
Session = sessionmaker(bind=engine)


class Advertisement(BaseModel):
    __tablename__ = 'advertisements'
    id = Column(Integer, primary_key=True)
    headline = Column(String, nullable=False)
    description = Column(String, nullable=False)
    time_create = Column(DateTime, default=datetime.now())
    Owner = Column(String, nullable=False)


class CreateAdvertisementModel(pydantic.BaseModel):
    headline: str
    description: str
    Owner: str


class HttpError(Exception):
    def __init__(self, status_code, error_message):
        self.status_code = status_code
        self.error_message = error_message


@app.errorhandler(HttpError)
def httr_error_handler(error):
    responce = jsonify({
        'error': error.error_message
    })
    responce.status_code = error.status_code
    return responce


class AdvertisementView(MethodView):

    def get(self, advertisement_id):
        with Session() as session:
            advertisement = session.query(Advertisement).get(advertisement_id)
            if advertisement is None:
                raise HttpError(404, 'advertisement not found')
            return jsonify({
                'id': advertisement.id,
                'headline': advertisement.headline,
                'description': advertisement.description,
                'time_create': advertisement.time_create,
                'owner': advertisement.Owner
            })

    def post(self):
        try:
            json_data_validate = CreateAdvertisementModel(**request.json).dict()
        except pydantic.ValidationError as er:
            raise HttpError(400, er.errors())

        with Session() as session:
            advertisement = Advertisement(**json_data_validate)
            session.add(advertisement)
            session.commit()
            return jsonify({
                'advertisement_id': advertisement.id
            })

    def patch(self, advertisement_id):
        with Session() as session:
            advertisement = session.query(Advertisement).get(advertisement_id)
            advertisement.headline = request.json.get('headline', advertisement.headline)
            advertisement.description = request.json.get('description', advertisement.description)
            advertisement.Owner = request.json.get('owner', advertisement.Owner)
            session.commit()
            return jsonify({
                'id': advertisement.id,
                'headline': advertisement.headline,
                'description': advertisement.description,
                'time_create': advertisement.time_create.timestamp(),
                'owner': advertisement.Owner
            })

    def delete(self, advertisement_id):
        with Session() as session:
            advertisement = session.query(Advertisement).get(advertisement_id)
            if advertisement is None:
                raise HttpError(404, 'advertisement not found')
            session.delete(advertisement)
            session.commit()
            return jsonify({
                'удаление': 'успешно'
            })


BaseModel.metadata.create_all(engine)

app.add_url_rule('/advertisements/<int:advertisement_id>', view_func=AdvertisementView.as_view('get_advertisement'),
                 methods=['GET'])
app.add_url_rule('/advertisements/<int:advertisement_id>', view_func=AdvertisementView.as_view('delete_advertisement'),
                 methods=['DELETE'])
app.add_url_rule('/advertisements/', view_func=AdvertisementView.as_view('create_advertisement'), methods=['POST'])
app.add_url_rule('/advertisements/<int:advertisement_id>', view_func=AdvertisementView.as_view('putch_advertisement'),
                 methods=['PATCH'])
app.run()
