import asyncio
import aiohttp
import csv
from bs4 import BeautifulSoup

# Cabeçalho para requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Limite de requisições simultâneas
MAX_CONCURRENT_REQUESTS = 30

# Função assíncrona para extrair detalhes de um filme
async def extract_movie_details(session, movie_link):
    try:
        async with session.get(movie_link, headers=HEADERS) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            title_tag = soup.find('h1')
            title = title_tag.find('span').get_text(strip=True) if title_tag and title_tag.find('span') else None

            date_tag = soup.find('a', href=lambda href: href and 'releaseinfo' in href)
            date = date_tag.get_text(strip=True) if date_tag else None

            rating_tag = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
            rating = rating_tag.get_text(strip=True) if rating_tag else None

            plot_tag = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
            plot_text = plot_tag.get_text(strip=True) if plot_tag else None

            if all([title, date, rating, plot_text]):
                print(title, date, rating, plot_text)
                return [title, date, rating, plot_text]

    except Exception as e:
        print(f"Erro ao acessar {movie_link}: {e}")

    return None

# Função para extrair links de filmes e chamar extract_movie_details
async def extract_movies(session, soup):
    movies_list = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_items = movies_list.find_all('li')
    movie_links = ['https://www.imdb.com' + item.find('a')['href'] for item in movies_items]

    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    results = []

    async def sem_task(link):
        async with sem:
            res = await extract_movie_details(session, link)
            if res:
                results.append(res)

    await asyncio.gather(*(sem_task(link) for link in movie_links))
    return results

# Função principal
async def main():
    url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            movies_data = await extract_movies(session, soup)

            # Salvar em CSV
            with open('movies.csv', mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Título', 'Data', 'Nota', 'Sinopse'])
                writer.writerows(movies_data)

    print(f"{len(movies_data)} filmes salvos em movies.csv")

# Rodar asyncio
if __name__ == '__main__':
    asyncio.run(main())
