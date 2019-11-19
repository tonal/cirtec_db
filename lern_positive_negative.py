# -*- codong: utf-8 -*-

from joblib import dump as jl_dump
import pandas as pd
import pymorphy2
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

from util_text import re_drop_no_wrd

DOC1 = 'text_rating_final.xlsx'   # 2015 размеченная выборка ~32000 фраз из проекта http://linis-crowd.org/


def main():
  df = pd.read_excel(
    DOC1, header=None, names=['seq', 'label'], usecols='A:B',)
  df.label = pd.to_numeric(df.label, errors='coerce')
  df.dropna(inplace=True)
  df.info()
  print(df.head())

  morph = pymorphy2.MorphAnalyzer()
  wrd_norm = {}
  get_norm = lambda w: (
    wrd_norm.get(w) or wrd_norm.setdefault(w, morph.parse(w)[0].normal_form))

  seq2text = lambda seq: ' '.join(map(get_norm, re_drop_no_wrd(seq).split()))

  df['text'] = df.seq.apply(seq2text)
  # df['text'] = df.seq
  print(df.head())

  cvect = CountVectorizer(max_features=5000, ngram_range=(1, 6))
  X = cvect.fit_transform(df.text)
  jl_dump(cvect, 'positive_negative_cvect.joblib')

  X_train, X_test, y_train, y_test = train_test_split(
    X, df.label, test_size=0.2, random_state=20191003, stratify=df.label)

  model = LogisticRegression(
    random_state=20190217, multi_class='auto', solver='lbfgs', max_iter=10000)
  # model = Ridge()

  model.fit(X_train, y_train)

  h = model.predict(X_test)

  print((h == y_test).mean())
  jl_dump(model, 'positive_negative_model.joblib')


if __name__ == '__main__':
  main()
