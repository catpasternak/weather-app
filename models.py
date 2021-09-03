import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Hotel(Base):
    """
    Keeps information about hotels.
    """
    __tablename__ = 'hotels'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    country = sa.Column(sa.String)
    city = sa.Column(sa.String)
    latitude = sa.Column(sa.Float)
    longitude = sa.Column(sa.Float)
    address = sa.Column(sa.String)

    def __repr__(self):
        return f'<{self.country} | {self.city} | {self.name}>'


class CityData(Base):
    """
    Represents one city per each country with geographic and weather information.
    Last 10 columns are intended for sequential days temperature lists starting from 5 days ago.
    """
    __tablename__ = 'temperature'
    id = sa.Column(sa.Integer, primary_key=True)
    country = sa.Column(sa.String)
    city = sa.Column(sa.String)
    latitude = sa.Column(sa.Float)
    longitude = sa.Column(sa.Float)
    historic_5 = sa.Column(sa.String)
    historic_4 = sa.Column(sa.String)
    historic_3 = sa.Column(sa.String)
    historic_2 = sa.Column(sa.String)
    historic_1 = sa.Column(sa.String)
    today = sa.Column(sa.String)
    forecast_1 = sa.Column(sa.String)
    forecast_2 = sa.Column(sa.String)
    forecast_3 = sa.Column(sa.String)
    forecast_4 = sa.Column(sa.String)

    def __repr__(self):
        return f'Temperature in {self.city}'


class MajorCity(Base):
    """
    Service class. Maps city to country.
    """
    __tablename__ = 'major_cities'
    country = sa.Column(sa.String, primary_key=True)
    city = sa.Column(sa.String)

    def __repr__(self):
        return f'<{self.country} | {self.city}>'
