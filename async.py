import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from aiolimiter import AsyncLimiter
import sys

# Adjust the event loop policy for Windows to avoid the RuntimeError: Event loop is closed
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch(session, url, limiter):
    # Wait for permission from the limiter before proceeding
    async with limiter:
        async with session.get(url) as response:
            return await response.text()

async def parse(url, session, limiter):
    try:
        html = await fetch(session, url, limiter)
        soup = BeautifulSoup(html, 'html.parser')
        container = soup.find("div", class_="chiptuning-files-models-overview chiptuning-files-models-overview--alt reveal")
        models = container.find_all("a")
        return [model.get("href") for model in models]
    except Exception as e:
        print(f"Error fetching or parsing {url}: {str(e)}")
        return []

async def main(urls):
    limiter = AsyncLimiter(50, 60)
    async with aiohttp.ClientSession() as session:
        tasks = [parse(url, session, limiter) for url in urls]
        results = await asyncio.gather(*tasks)
    return [link for links in results for link in links]

async def write_to_file(links):
    with open("links.txt", "a") as f:
        for link in links:
            f.write(f"{link}\n")

async def batch_scrape(df):
    batch_size = 50
    for i in range(0, len(df), batch_size):
        urls = df.iloc[i:i+batch_size]['link'].tolist()
        links = await main(urls)
        await write_to_file(links)

def load_and_run():
    # Read the Excel file into a DataFrame
    df = pd.read_excel("models.xlsx")[:150]
    asyncio.run(batch_scrape(df))

if __name__ == "__main__":
    load_and_run()
