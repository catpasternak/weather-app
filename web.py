from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_

from models import *


def configure_app(db_path='sqlite:///db.sqlite3'):
    """
    Creates Flask application
    :param db_path: path to database
    :return: Flask application
    """
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = db_path
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db = SQLAlchemy(app)

    @app.route('/')
    def home():
        """
        Home page with links to hotels page and cities page
        """
        return render_template('home.html', title='Weather application home page')

    @app.route('/hotels')
    def hotels():
        """
        Hotels table with pagination and filter
        """
        hotels = []
        major_cities = db.session.query(MajorCity).all()
        for major_city in major_cities:
            city_hotels = db.session.query(Hotel).filter(
                and_(
                    Hotel.country == major_city.country,
                    Hotel.city == major_city.city
                )
            ).all()
            hotels.extend(city_hotels)
        return render_template('hotels.html', title='Hotels data', hotels=hotels)

    @app.route('/cities')
    def cities():
        """
        Cities table with temperature plots
        """
        cities = db.session.query(CityData).all()
        return render_template('cities.html', title='Cities data', cities=cities)

    return app
