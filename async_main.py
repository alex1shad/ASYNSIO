import os
from dotenv import load_dotenv
import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm.session import sessionmaker
import models as m


load_dotenv()

http = 'https://swapi.dev/api/people/'

db_type = os.getenv('DB_TYPE')
login = os.getenv('LOGIN')
password = os.getenv('PASSWORD')
hostname = os.getenv('HOSTNAME')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

DSN = f'{db_type}+asyncpg://{login}:{password}@{hostname}:{db_port}/{db_name}'
engine = create_async_engine(DSN)

Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def paste_to_db(hero_list):
    async with Session() as session:
        heros_as_objects = [m.Swapi(data=item) for item in hero_list]
        session.add_all(heros_as_objects)
        await session.commit()


async def download_links(links_list, client_session):
    coroutines = [client_session.get(link) for link in links_list]
    http_response = await asyncio.gather(*coroutines)
    json_coroutines = [http_response.json() for http_response in http_response]
    return await asyncio.gather(*json_coroutines)


async def get_hero(hero_id, client_session):
    async with client_session.get(f'{http}{hero_id}/') as response:
        json_data = await response.json()
        films_links = json_data.get('films', [])
        films_coroutine = download_links(films_links, client_session)
        homeworld_links = [json_data.get('homeworld')]
        homeworld_coroutine = download_links(homeworld_links, client_session)
        species_links = json_data.get('species', [])
        species_coroutine = download_links(species_links, client_session)
        starships_links = json_data.get('starships', [])
        starships_coroutine = download_links(starships_links, client_session)
        vehicles_links = json_data.get('vehicles', [])
        vehicles_coroutine = download_links(vehicles_links, client_session)
        fields = await asyncio.gather(films_coroutine,
                                      species_coroutine,
                                      starships_coroutine,
                                      vehicles_coroutine,
                                      homeworld_coroutine)
        films, species, starships, vehicles, homeworld = fields
        json_data['id'] = hero_id
        json_data['films'] = [film['title'] for film in films]
        json_data['species'] = [specie['name'] for specie in species]
        json_data['starships'] = [starship['name'] for starship in starships]
        json_data['vehicles'] = [vehicle['name'] for vehicle in vehicles]
        json_data['homeworld'] = [home['name'] for home in homeworld][0]
        return json_data


async def main():
    async with engine.begin() as con:
        await con.run_sync(m.Base.metadata.create_all)

    async with aiohttp.ClientSession() as client_session:
        for task in [i * 5 for i in range(0, 3)]:
            task_range = range(task+1, task + 6)
            coroutines = [get_hero(i, client_session) for i in task_range]
            result = await asyncio.gather(*coroutines)
            asyncio.create_task(paste_to_db(result))

    all_tasks = asyncio.all_tasks()
    all_tasks = all_tasks - {asyncio.current_task()}
    await asyncio.gather(*all_tasks)


asyncio.run(main())
