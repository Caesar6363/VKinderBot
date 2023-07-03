import sqlalchemy as sq
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


Base = declarative_base()
engine = sq.create_engine('postgresql://username:password@localhost:5432/VKinder')
Session = sessionmaker(bind=engine)
session = Session()


class User(Base):
    __tablename__ = 'user'
    id = sq.Column(sq.Integer, primary_key=True)
    vk_id = sq.Column(sq.Integer)
    first_name = sq.Column(sq.String)
    last_name = sq.Column(sq.String)
    age_from = sq.Column(sq.Integer)
    age_to = sq.Column(sq.Integer)
    target_gender = sq.Column(sq.Integer)
    city = sq.Column(sq.String)


class Partner(Base):
    __tablename__ = 'partner'
    id = sq.Column(sq.Integer, primary_key=True)
    vk_id = sq.Column(sq.Integer)
    first_name = sq.Column(sq.String)
    last_name = sq.Column(sq.String)
    id_User = sq.Column(sq.Integer, sq.ForeignKey('user.id'))
    user = relationship(User)


class Favorite(Base):
    __tablename__ = 'favorite'
    id = sq.Column(sq.Integer, primary_key=True)
    vk_id = sq.Column(sq.Integer)
    first_name = sq.Column(sq.String)
    last_name = sq.Column(sq.String)
    id_User = sq.Column(sq.Integer, sq.ForeignKey('user.id'))
    user = relationship(User)


class UserPosition(Base):
    __tablename__ = 'user_position'
    id = sq.Column(sq.Integer, primary_key=True)
    id_User = sq.Column(sq.Integer, sq.ForeignKey('user.id'))
    vk_id = sq.Column(sq.Integer)
    position = sq.Column(sq.SmallInteger)
    offset = sq.Column(sq.SmallInteger)
    user = relationship(User)


def create_tables():
    try:
        Base.metadata.create_all(engine)
    except Exception as e:
        print(e)


def add_user(user):
    try:
        session.expire_on_commit = False
        if isinstance(user, User) and session.query(User.vk_id).filter(User.vk_id == user.vk_id).first() is not None:
            return
        elif isinstance(user, UserPosition) and \
                session.query(UserPosition.vk_id).filter(UserPosition.vk_id == user.vk_id).first() is not None:
            update(user.vk_id, UserPosition, position=1)
        else:
            session.add(user)
            session.commit()
    except Exception as e:
        print(e)


def update(user_id, target_table, **kwargs):
    try:
        db_user_id = get_db_id(user_id)
        if target_table is User:
            session.query(target_table).filter(target_table.id == db_user_id).update({**kwargs})
        else:
            session.query(target_table).filter(target_table.id_User == db_user_id).update({**kwargs})
        session.commit()
    except Exception as e:
        print(e)


def delete_user(partner_id):
    try:
        session.expire_on_commit = False
        session.query(Favorite).filter(Favorite.vk_id == partner_id).delete()
        session.commit()
    except Exception as e:
        print(e)


def view_favorites(user_id):
    links = []
    try:
        db_user_id = get_db_id(user_id)
        partners_query = session.query(Favorite.vk_id).filter(Favorite.id_User == db_user_id).all()
        for link in partners_query:
            links.append(link[0])
        return links
    except Exception as e:
        print(e)


def avoid_list(user_id):
    links = []
    try:
        partners_query = session.query(Partner.vk_id).filter(Partner.id_User == user_id).all()
        for link in partners_query:
            links.append(link[0])
        return links
    except Exception as e:
        print(e)


def get_position(user_id):
    try:
        position = session.query(UserPosition.position).filter(UserPosition.vk_id == user_id).first()
        if not position:
            return_count = 0
        else:
            return_count = position[0]
        return return_count
    except Exception as e:
        print(e)


def get_offset(user_id):
    try:
        offset = session.query(UserPosition.offset).filter(UserPosition.vk_id == user_id).first()
        if not offset:
            return_count = 0
        else:
            return_count = offset[0]
        return return_count
    except Exception as e:
        print(e)


def get_db_id(user_id):
    try:
        return session.query(User.id).filter(User.vk_id == user_id).first()
    except Exception as e:
        print(e)


def get_city(user_id):
    try:
        return session.query(User.city).filter(User.vk_id == user_id).first()
    except Exception as e:
        print(e)


def get_sex(user_id):
    try:
        return session.query(User.target_gender).filter(User.vk_id == user_id).first()
    except Exception as e:
        print(e)


def get_age_from(user_id):
    try:
        return session.query(User.age_from).filter(User.vk_id == user_id).first()
    except Exception as e:
        print(e)


def get_age_to(user_id):
    try:
        return session.query(User.age_from).filter(User.vk_id == user_id).first()
    except Exception as e:
        print(e)


def get_partner_id():
    try:
        return session.query(Partner.vk_id).order_by(Partner.id.desc()).first()
    except Exception as e:
        print(e)


def get_partner_first_name():
    try:
        return session.query(Partner.first_name).order_by(Partner.id.desc()).first()
    except Exception as e:
        print(e)


def get_partner_last_name():
    try:
        return session.query(Partner.last_name).order_by(Partner.id.desc()).first()
    except Exception as e:
        print(e)
