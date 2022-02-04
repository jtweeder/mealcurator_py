import requests
import numpy as np
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, words
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer


class url_reader():
    def __init__(self, url):
        self.url = url
        self.soup = self._make_soup()

    def raw_to_mstr(self):
        # TODO: Build out insert statement to target master table
        return tuple()

    def _make_soup(self):
        page = requests.get(self.url)
        if not page.ok:
            raise ValueError(f'URL did not resolve to OK: {page.status_code}')
        return BeautifulSoup(page.content, 'html.parser')

    def _make_tkns(self):
        raw_tkns = [w for w in word_tokenize(self.soup.get_text().lower())
                    if w.isalpha() and w not in stopwords.words('english')]
        lemmatizer = WordNetLemmatizer()
        lemented = [lemmatizer.lemmatize(w) for w in raw_tkns]
        eng_words = set(words.words())
        eng_lemented = [w for w in lemented if w in eng_words]

        # Later move this out to batch process the vocab better
        def dummy(doc):
            return doc
        countVect = CountVectorizer(tokenizer=dummy,
                                    preprocessor=dummy,
                                    token_pattern=None)
        w_matrix = countVect.fit_transform([eng_lemented])
        voc_dict = {countVect.get_feature_names_out()[i]: w_matrix.toarray()[0][i]
                    for i in np.arange(0, len(w_matrix.toarray()[0]))}
        return voc_dict
