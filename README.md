# Заполняем базу данных

## Публикации и контексты

Заполняются из текста со следующей страницы: [Sergey-Sinelnikov-Murylev ru](http://cirtec.ranepa.ru/cgi/spadist4bundle.cgi?code=linked_papers&c=Sergey-Sinelnikov-Murylev)
или с [Sergey-Sinelnikov-Murylev org](http://cirtec.repec.org/cgi/spadist4bundle.cgi?code=linked_papers&c=Sergey-Sinelnikov-Murylev)

На странице находятся блоки данных о публикации.

Каждый начинается со ссылки на публикацию вида:
`https://socionet.ru/publication.xml?h=repec:rnp:ecopol:1532`
  
Где `repec:rnp:ecopol:1532` - уникальный ID контекста.

Далее идёт список фрагментов со списком контекстов цитирования.


## Запросы к серверу

### А Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
```http request
GET /cirtec/frags/
```

### А Распределение цитирований по 5-ти фрагментам для отдельных публикаций. #заданного автора.
```http request
GET /cirtec/frags/publications/
```

### А Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
```http request
GET /cirtec/frags/cocitauthors/
```
Параметры:
  - **topn**: число интерисующих авторов из topN. Можно не указывать

### А Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»
```http request
GET /cirtec/frags/ngramm/
```
Параметры:
  - **topn**: число интерисующих фраз из topN. Если не указывать topn=10.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### A Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»
```http request
GET /cirtec/frags/topics/
```
Параметры:
  - **topn**: число интерисующих топиков из topN. Можно не указывать


### Б Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»
```http request
GET /cirtec/frags/cocitauthors/ngramms/
```
Параметры:
  - **topn**: число интерисующих авторов из topN. Можно не указывать
  - **topn_gramm**: число интерисующих фраз из topN. Если не указывать topn=500. 
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### Топ N Со-цитируемых авторов 
```http request
GET /cirtec/top/cocitauthors/
```
Параметры:
  - **topn**: число интерисующих авторов из topN. Можно не указывать

### Топ N фраз 
```http request
GET /cirtec/top/ngramm/
```
Параметры:
Параметры:
  - **topn**: число интерисующих фраз из topN. Можно не указывать.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### Топ N топиков 
```http request
GET /cirtec/top/topics/
```
Параметры:
  - **topn**: число интерисующих топиков из topN. Можно не указывать

