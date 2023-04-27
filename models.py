import sqlalchemy as sq
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Swapi(Base):
    __tablename__ = 'swapi'

    id = sq.Column(sq.Integer, primary_key=True)
    data = sq.Column(sq.JSON)
