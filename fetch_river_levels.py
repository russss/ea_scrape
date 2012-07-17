import requests
import datetime
from lxml.html.soupparser import fromstring
from lxml.etree import tostring
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, DateTime, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from time import sleep

# area_id -> [station_id]
SOURCES = {120732: [6258, # Ouzel at Milton Keynes M1 Ultrasonic
                    6165, # River Ouzel at Willen
                    6164, # Broughton Brook at Broughton
                    6162, # River Ouse at Newport Pagnell
                    6266  # Ouzel at Caldecotte
                    ]}

def get_data(area_id, station_id):
    url = 'http://www.environment-agency.gov.uk/homeandleisure/floods/riverlevels/%s.aspx?stationId=%s'
    r = requests.get(url % (area_id, station_id))

    data = fromstring(r.text)

    date_text = data.find('.//div[@id="content"]/div/div/p').text

    time, date = date_text.strip('Last updated ').split(' on ')

    day, month, year = map(int, date.split('/'))
    hour, minute = map(int, time.split(':'))

    height = float(data.find(".//div[@id='station-detail-left']//div[@class='plain_text']/p"
                                                                ).text.split(' is ')[1].split(' ')[0])

    return datetime.datetime(year, month, day, hour, minute), height

engine = create_engine('postgresql://russ:russ@127.0.0.1:5433/russ')
connection = engine.connect()

meta = MetaData(bind=connection)
RiverData = Table('river_data', meta,
                Column('id', Integer, primary_key=True),
                Column('area_id', Integer, nullable=False),
                Column('station_id', Integer, nullable=False),
                Column('date', DateTime, nullable=False),
                Column('level', Float, nullable=False),
                UniqueConstraint('area_id', 'station_id', 'date')
                )
meta.create_all()

for area_id, stations in SOURCES.iteritems():
    for station_id in stations:
        date, level = get_data(area_id, station_id)
        try:
            connection.execute(RiverData.insert().values(
                        area_id=area_id, station_id=station_id, date=date, level=level))
        except IntegrityError:
            pass
        sleep(2)

connection.close()
