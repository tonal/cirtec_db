# Заполняем базу данных

## Запросы к серверу

В большинстве запросов можно ограничить выдачу:
  - **topn**: число интересующих элементов.

Везде, где в запросах встречаются «фразы» можно указать арность и тип с помощью 
следующих параметров: 
  - **nka**: арность фразы.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы
В случае, если тип не указан, он выдаётся ответе для каждой фразы.

Везде, где в запросах встречаются «топики контекстов цитирований» можно указать 
минимальную вероятность учитываемых топиков: 
  - **probability**: минимальная вероятность

### А Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
```http request
GET /cirtec/frags/
```

### А Распределение цитирований по 5-ти фрагментам для отдельных публикаций.
```http request
GET /cirtec/frags/publications/
```

### А Кросс-распределение «со-цитируемые авторы» по публикациям
```http request
GET /cirtec/publ/publications/cocitauthors/
```
Параметры:
  - **topn**: число интересующих со-цитируемых авторов из topN. Можно не указывать
  - **topn_cocitauthors**: число интересующих со-цитируемых кросс-авторов из topN. Можно не указывать

### А Кросс-распределение «фразы из контекстов цитирований» по публикациям
```http request
GET /cirtec/publ/publications/ngramms/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Если не указывать topn=10.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы
  
### А Кросс-распределение «топики контекстов цитирований» по публикациям
```http request
GET /cirtec/publ/publications/topics/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать

### А Кросс-распределение «публикации» - «со-цитируемые авторы»
```http request
GET /cirtec/publ/cocitauthors/cocitauthors/
```
Параметры:
  - **topn**: число интересующих со-цитируемых авторов из topN. Можно не указывать
  - **topn_cocitauthors**: число интересующих со-цитируемых кросс-авторов из topN. Можно не указывать

### А Количества употребления фраз в контексте
```http request
/cirtec/cnt/publications/ngramms/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Можно не указывать.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы. Можно не указывать

В ответе:
  - **count** - общее количество повторений фразы в контекстах публикации
  - **count_conts** - количество контекстов публикации в которых фраза встречалась
Если не задан параметр **ltype**
  - **type**: *lemmas* или нелеммизированные *nolemmas* 

### А Распределение «со-цитируемые авторы» по 5-ти фрагментам
```http request
GET /cirtec/frags/cocitauthors/
```
Параметры:
  - **topn**: число интересующих авторов из topN. Можно не указывать

### А Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
```http request
GET /cirtec/frags/cocitauthors/cocitauthors/
```
Параметры:
  - **topn**: число интересующих со-цитируемых авторов из topN. Можно не указывать
  - **topn_cocitauthors**: число интересующих со-цитируемых кросс-авторов из topN. Можно не указывать

### А Кросс-распределение «публикации» - «со-цитируемые авторы»
```http request
GET /cirtec/pubs/cocitauthors/cocitauthors/
```
Параметры:
  - **topn**: число интересующих со-цитируемых авторов из topN. Можно не указывать
  - **topn_cocitauthors**: число интересующих со-цитируемых кросс-авторов из topN. Можно не указывать

### А Распределение «5 фрагментов» - «фразы из контекстов цитирований»
```http request
GET /cirtec/frags/ngramms/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Если не указывать topn=10.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### А Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»
```http request
GET /cirtec/frags/ngramms/ngramms/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Если не указывать topn=10.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### А Кросс-распределение «публикации» - «фразы из контекстов цитирований»
```http request
GET /cirtec/publ/ngramms/ngramms/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Если не указывать topn=10.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### A Распределение «5 фрагментов» - «топики контекстов цитирований»
```http request
GET /cirtec/frags/topics/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать

### A Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»
```http request
GET /cirtec/frags/topics/topics/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать

### A Кросс-распределение «публикации» - «топики контекстов цитирований»"
```http request
GET /cirtec/publ/topics/topics/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать

### Б Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»
```http request
GET /cirtec/frags/cocitauthors/ngramms/
```
Параметры:
  - **topn**: число интересующих авторов из topN. Можно не указывать
  - **topn_gramm**: число интересующих фраз из topN. Если не указывать topn=500. 
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 


### Б Кросс-распределение «со-цитирования» - «топики контекстов цитирований»
```http request
GET /cirtec/frags/cocitauthors/topics/
```
Параметры:
  - **topn**: число интересующих авторов из topN. Можно не указывать
  - **topn_topics**: число интересующих фраз из topN. Можно не указывать 


### В Кросс-распределение «фразы» - «со-цитирования»
```http request
GET /cirtec/frags/ngramms/cocitauthors/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Если не указывать topn=10.
  - **topn_cocitauthors**:  число интересующих авторов из topN. Можно не указывать
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 


### В Кросс-распределение «фразы» - «топики контекстов цитирований»
```http request
GET /cirtec/frags/ngramms/topics/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Если не указывать topn=10.
  - **topn_topics**:  число интересующих топиков из topN. Можно не указывать
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 


### Г Кросс-распределение «топики» - «со-цитирования»
```http request
GET /cirtec/frags/topics/cocitauthors/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать
  - **topn_cocitauthors**:  число интересующих авторов из topN. Можно не указывать


### Г Кросс-распределение «топики» - «фразы»
```http request
GET /cirtec/frags/topics/ngramms/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать
  - **topn_gramm**: число интересующих фраз из topN. Если не указывать topn=500. 
  - **topn_crpssgramm**: Ограничение на количество фраз для топика. Можно не указывать
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 


### Топ N Со-цитируемых авторов 
```http request
GET /cirtec/top/cocitauthors/
```
Параметры:
  - **topn**: число интересующих авторов из topN. Можно не указывать

### Топ N со-цитируемых авторов по публикациям 
```http request
GET /cirtec/top/cocitauthors/publications/
```
Параметры:
  - **topn**: число интересующих авторов из topN. Можно не указывать

### Топ N фраз 
```http request
GET /cirtec/top/ngramm/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Можно не указывать.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### Топ N фраз по публикациям 
```http request
GET /cirtec/top/ngramm/publications/
```
Параметры:
  - **topn**: число интересующих фраз из topN. Можно не указывать.
  - **nka**: арность фразы. Можно не указывать.
  - **ltype**: использовать леммизированые *lemmas* или нелеммизированные *nolemmas* фразы 

### Топ N топиков 
```http request
GET /cirtec/top/topics/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать

### Топ N топиков по публикациям 
```http request
GET /cirtec/top/topics/publications/
```
Параметры:
  - **topn**: число интересующих топиков из topN. Можно не указывать

### Топ N фраз
```http request
GET /cirtec/top/ngramms/
```

### Топ N фраз по публикациям
```http request
GET /cirtec/top/ngramms/publications/
```

### Топ N фраз
```http request
GET /cirtec/cnt/ngramms/
```

### Топ N фраз по публикациям
```http request
GET /cirtec/cnt/publications/ngramms/
```

### Топ N бандлов
```http request
GET /cirtec/top/ref_bundles/
```

### Топ N авторов бандлов
```http request
GET /cirtec/top/ref_authors/
```

### Топ N авторов бандлов по публикациям
```http request
GET /cirtec/pubs/ref_authors/
```

### Кросс-распределение бандлов и топиков с фразами 
```http request
GET /cirtec/ref_bund4ngramm_tops/
```

### Кросс-распределение авторов бандлов и топиков с фразами 
```http request
GET /cirtec/ref_auth4ngramm_tops/
```

### Распределение тональности позитивный/негативный по публикациям
```http request
GET /cirtec/pos_neg/pubs/
```

### Распределение тональности позитивный/негативный по бандлам
```http request
GET /cirtec/pos_neg/ref_bundles/
```

### Распределение тональности позитивный/негативный по авторам бандлов
```http request
GET /cirtec/pos_neg/ref_authors/
```

### Распределение бандлов по фрагментам
```http request
GET /cirtec/frags/ref_bundles/
```

### Распределение авторов бандлов по фрагментам
```http request
GET /cirtec/frags/ref_authors/
```

### информация о публикациях
```http request
GET /cirtec/publications/
```

### Общее распределение классов тональности для контекстов из всех публикаций заданного автора
```http request
GET /cirtec/pos_neg/contexts/
```

### Тональность для топиков
```http request
GET /cirtec/pos_neg/topics/
```

### Тональность для фраз
```http request
GET /cirtec/pos_neg/ngramms/
```

### Тональность для со-цитируемых авторов
```http request
GET /cirtec/pos_neg/cocitauthors/
```

### Распределение тональности контекстов по 5-ти фрагментам
```http request
GET /cirtec/frags/pos_neg/contexts/
```

### Со-цитируемые авторы, кросс-распределение тональности их со-цитирований и распределение по 5-ти фрагментам
```http request
GET /cirtec/frags/pos_neg/cocitauthors/cocitauthors/
```

### Топ N со-цитируемых бандлов
```http request
GET /cirtec_dev/top/cocitrefs/
```

### Кросс-распределение «со-цитируемые бандлов» 
```http request
GET /cirtec_dev/top/cocitrefs/cocitrefs/
```

### Для каждого из 5-ти фрагментов публикаций топ авторов референсов.
```http request
GET /cirtec_dev/by_frags/ref_authors/
```

### Топ N авторов бандлов с детализацией по бандлам
```http request
GET /cirtec_dev/top_detail_bund/ref_authors/
```
 
