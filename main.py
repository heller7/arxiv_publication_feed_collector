import feedparser
import sqlite3
import ollama
from datetime import datetime
import re


def fetch_arxiv_feed(max_results=10):
    base_url = "http://export.arxiv.org/api/query?"
    categories = ['math.CO', 'math.HO', 'math.OC', 'cs.CC', 'cs.DM', 'cs.LG', 'cs.SI', 'cs.GT', 'cs.CY', 'stat.OT']
    query = f'search_query=cat:{"+OR+".join(categories)}&start=0&max_results={max_results}&sortBy=submittedDate&sortOrder=descending'

    feed = base_url + query

    feed = feedparser.parse(feed)
    return feed.entries


def categorize_via_ollama(title, abstract, model_name="llama2:latest"):
    prompt = f"""
                You are an expert academic assistant that categorizes research papers based on their content.
                
                Paper Title: "{title}"
                
                Abstract: "{abstract}"
                
                Please provide a concise list of relevant categories for this paper. The list has to be comma-separated. 
                Under no circumstances write more than the categories.
                """.strip()
    response = ollama.generate(model=model_name, system="You are a paper categorization assistant.", prompt=prompt)
    categoriesResponse = response["response"]
    categories = process_categories(categoriesResponse)
    return categories if categories else ["Uncategorized"]


def process_categories(response):
    lines = response.splitlines()
    categories = []

    for line in lines:
        match = re.match(r'^\*\s*(.+)', line)
        if match:
            category = match.group(1).strip()
            categories.append(category)

    return categories if categories else ["Uncategorized"]


def create_db():
    conn = sqlite3.connect('arxiv_publications.sqlite')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            authors TEXT NOT NULL,
            abstract TEXT,
            categories TEXT,
            arxiv_category TEXT,
            link TEXT NOT NULL,
            date_fetched TEXT NOT NULL,
            publication_date TEXT,
            doi TEXT,
            UNIQUE(doi, title, authors)
        )
    ''')

    conn.commit()
    conn.close()


def publication_exists(doi, title, authors):
    conn = sqlite3.connect('arxiv_publications.sqlite')
    cursor = conn.cursor()

    cursor.execute('''
        SELECT 1 FROM publications WHERE doi = ? OR (title = ? AND authors = ?)
    ''', (doi, title, authors))

    exists = cursor.fetchone() is not None
    conn.close()

    return exists


def insert_publication(title, authors, abstract, categories, arxiv_category, doi, link, publication_date):
    conn = sqlite3.connect('arxiv_publications.sqlite')
    cursor = conn.cursor()

    cursor.execute('''
            INSERT INTO publications (source, title, authors, abstract, categories, arxiv_category, link, date_fetched, publication_date, doi)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
        "Arxiv",
        title,
        authors,
        abstract,
        ', '.join(categories),
        arxiv_category,
        link,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        publication_date,
        doi
    ))

    conn.commit()
    conn.close()


def main():
    create_db()
    entries = fetch_arxiv_feed()
    for entry in entries:
        title = entry.title.strip()
        authors = ', '.join([author.name for author in entry.authors])
        abstract = entry.summary.strip()
        arxiv_category = entry.tags[0]['term']
        print(f"Arxiv Category: {arxiv_category}\n")
        link = entry.link
        print(f"Link: {link}\n")
        doi = entry.id
        print(f"DOI: {doi}\n")
        if not publication_exists(doi, title, authors):
            print(f"Categorizing paper: {title}")
            categories = categorize_via_ollama(title, abstract)
            print(f"Categories: {categories}\n")

            insert_publication(title=title,
                               authors=authors,
                               abstract=abstract,
                               categories=categories,
                               doi=doi,
                               arxiv_category=arxiv_category,
                               link=link,
                               publication_date=entry.published)
        else:
            print(f"Publication already exists: {title}")


if __name__ == "__main__":
    main()
