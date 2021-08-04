import asyncio
import re
import datetime
from pydantic import ValidationError
from aiohttp import ClientSession
from asyncio import Queue
from models import Article


ARTICLE_LIST_URL = "https://mapping-test.fra1.digitaloceanspaces.com/data/list.json"
ARTICLE_DETAIL_URL = "https://mapping-test.fra1.digitaloceanspaces.com/data/articles"
ARTICLE_MEDIA_URL = "https://mapping-test.fra1.digitaloceanspaces.com/data/media"

async def fetch_articles():
    async with ClientSession() as session:
        async with session.get(ARTICLE_LIST_URL) as resp:
            res = await resp.json()
            article_ids = [item["id"] for item in res if item.get("id")]
            print(f"article ids are {article_ids}")
            tasks = (
                fetch_article(session, article_id)
                for article_id in article_ids
            )
            return await asyncio.gather(*tasks)


async def fetch_article(session, article_id):
    article_url = f"{ARTICLE_DETAIL_URL}/{article_id}.json"
    async with session.get(article_url) as resp:
        res = await resp.json()

        media_ids = [section["id"] for section in res["sections"] if section["type"] == "media"]
        medias = await fetch_media(session, article_id)
        medias = {
            media["id"]: MediaHandler(media).validate() for media in medias if media["id"] in media_ids
        }
        
        data = ArticleHandler(res, medias).validate()
        data["url"] = article_url

        try:
            article = Article(**data)
            print(article)
        except ValidationError as e:
            print(e)


async def fetch_media(session, article_id):
    async with session.get(f"{ARTICLE_MEDIA_URL}/{article_id}.json") as resp:
        try:
            return await resp.json()
        except Exception as e:
            return []


async def create_scheduler(queue, interval=60*5):
    index = 0
    while True:
        await queue.put(index)
        await asyncio.sleep(interval)
        index += 1


async def worker(queue):
    while True:
        index = await queue.get()
        print(f"Fetching {index}th article list...")
        await fetch_articles()
        print(f"Accomplished {index}th fetching api...")
        queue.task_done()


async def main():
    queue = Queue()
    scheduler = [asyncio.create_task(create_scheduler(queue))]
    consumers = [asyncio.create_task(worker(queue))]
    await asyncio.gather(*scheduler)
    await queue.join()
    for c in consumers:
        c.cancel()


class DateValidatorMixin:
    def validate_date(self, data_str, format):
        if data_str is None:
            return datetime.datetime.now().isoformat()
        else:
            date_obj = datetime.datetime.strptime(data_str, format)
            return date_obj.isoformat()


class ArticleHandler(DateValidatorMixin):
    def __init__(self, data, media) -> None:
        self.data = data
        self.media = media

    def update_section(self, section):
        if section["type"] == "media":
            return {**section, **self.media[section["id"]]}

        if section["type"] != "text":
            return section
        clean = re.compile('<.*?>')
        stripped_text = re.sub(clean, '', section["text"])
        section["text"] = stripped_text
        return section

    def validate(self):
        self.data["publication_date"] = self.validate_date(
            self.data.get("pub_date"), "%Y-%m-%d-%H;%M;%S")
        self.data["modification_date"] = self.validate_date(
            self.data.get("mod_date"), "%Y-%m-%d-%H:%M:%S")
        self.data["sections"] = list(map(self.update_section, self.data["sections"]))
        return self.data


class MediaHandler(DateValidatorMixin):
    def __init__(self, data) -> None:
        self.data = data

    def validate(self):
        self.data["publication_date"] = self.validate_date(
            self.data.get("pub_date"), "%Y-%m-%d-%H;%M;%S")
        self.data["modification_date"] = self.validate_date(
            self.data.get("mod_date"), "%Y-%m-%d-%H:%M:%S")
        return self.data


if __name__ == "__main__":
    asyncio.run(main())
