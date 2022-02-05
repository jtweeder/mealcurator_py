import json
import requests
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


class url_reader():

    def __init__(self, url):
        """
        Digest a given URL and make ready for insert into mstr table

        Parameters
        ----------
        url : string
            URL address of website to scrape
        """
        self.url = url
        self.soup = self._make_soup()
        self.title = self.soup.title.string
        self.learned_words = self._make_tkns()

    def raw_to_mstr(self):
        """
        Parameters
        ----------
        None

        Returns
        -------
        (title, learned_words)
            title : str
                Titls scarped from HTML of given URL
            leanred_words : str
                JSON Readable string of lemented words
        """
        return self.title, self.learned_words

    def _make_soup(self):
        page = requests.get(self.url)
        if not page.ok:
            raise ValueError(f'URL did not resolve to OK: {page.status_code}')
        return BeautifulSoup(page.content, 'html.parser')

    def _make_tkns(self):
        # Returns JSON represtnation of lemented word list
        raw_tkns = [w for w in word_tokenize(self.soup.get_text().lower())
                    if w.isalpha() and w not in stopwords.words('english')]
        lemmatizer = WordNetLemmatizer()
        lemented = [lemmatizer.lemmatize(w) for w in raw_tkns]
        return json.dumps(lemented)
