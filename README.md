

# Поиск фильмов

## Содержание

- [Анализ открытых источников и ресурсов с фильмами](#анализ-открытых-источников-и-ресурсов-с-фильмами)
- [Сбор данных](#сбор-данных)
- [Выбор используемых технологий](#выбор-используемых-технологий)
- [План реализации](#план-реализации)
- [Разработка бекенд части](#разработка-бекенд-части)
  - [Создание системы для поиска фильмов](#создание-системы-для-поиска-фильмов)
  - [Создание системы для рекомендации фильмов](#создание-системы-для-рекомендации-фильмов)
- [Создание сайта и пользовательский путь](#создание-сайта-и-пользовательский-путь)


## Анализ открытых источников и ресурсов с фильмами

В качестве основного источника данных выбран **Кинопоиск**. Этот ресурс отличается высоким уровнем детализации информации — здесь представлены не только рейтинги с различных площадок, постеры в разных форматах и подробные описания фильмов, но и данные о сериалах и аниме, что является важным для локальной аудитории.

### Сравнение с аналогами

- **IMDb:**  
  IMDb является мировым стандартом для поиска фильмов и сериалов, однако его база данных ориентирована на международную аудиторию. Кинопоиск, напротив, имеет более глубокое покрытие отечественного кинопроизводства, а также предоставляет дополнительные локальные рейтинги и отзывы, что делает его незаменимым для пользователей, интересующихся российским кино.

- **The Movie Database (TMDb):**  
  TMDb обладает открытым API и широкими возможностями для разработчиков, но его информация часто носит более универсальный характер и может быть менее адаптирована для русскоязычной аудитории. Кинопоиск предлагает данные, специально подобранные для российского рынка, с акцентом на локальные реалии и предпочтения зрителей.

- **Rotten Tomatoes:**  
  Rotten Tomatoes сосредоточен в первую очередь на критических отзывах и мнениях пользователей из США, что может не всегда отражать вкусы и особенности отечественного кинематографа. Кинопоиск, напротив, включает рейтинги как международных, так и российских критиков, что позволяет получить более комплексную оценку фильма с учётом местного контекста.

| **Критерий**              | **Кинопоиск**                                                | **IMDb**                                               | **TMDb**                                          | **Rotten Tomatoes**                          |
|---------------------------|--------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------|----------------------------------------------|
| **Ориентация аудитории**  | Русскоязычная, с акцентом на отечественный кинематограф      | Международная, глобальная база данных                  | Международная, универсальные данные               | Американская, с упором на критические обзоры   |
| **Детализация информации**| Высокая: подробные описания, рейтинги, постеры, новости        | Обширная информация, но меньше локальной специфики      | Хорошая: большое количество медиа-материалов       | Фокус на рейтингах и критических отзывах      |
| **Локальные данные**      | Да, включает российский кинематограф и локальные рейтинги       | Нет, преимущественно глобальные данные                  | Нет, универсальные данные                         | Нет, ориентировано на американский рынок       |
| **API доступность**       | Да, через [kinopoisk.dev](https://kinopoisk.dev/)              | Да, но с ограничениями                                 | Да, но с ограничениями                                   | Да, но с существенными ограничениями           |
| **Дополнительные функции**| Новости, локальные рейтинги, аналитика                        | Биографии, история производства, пользовательские рейтинги | Сообщество, пользовательские обзоры                | Метакритика, система оценок критиков           |


Таким образом, выбор Кинопоиска обусловлен его уникальной способностью предоставлять подробные и актуальные данные, адаптированные под нужды российской аудитории, а также возможностью параллельного доступа к данным с помощью API (через [kinopoisk.dev](https://kinopoisk.dev/)). Это делает его более подходящим для проектов, ориентированных на русскоязычных пользователей, чем глобальные аналоги.


Для доступа к данным используется сервис [kinopoisk.dev](https://kinopoisk.dev/). Благодаря возможности использовать несколько API-ключей и параллельной обработке запросов, удалось получить данные о 218750 произведениях, выпущенных до 2025 года и имеющих непустые описания.

> **Важно:** При выкачивании данных также получались картины, которые ещё не вышли или имели пустое описание. Такие данные не подходят для обучения модели поиска по описанию, жанрам и для работы рекомендательной системы, поэтому они были отфильтрованы.

## Сбор данных

Для получения данных были созданы 8 API-ключей.  
Всего в Кинопоиске доступно 32 жанра. Данные выкачиваются постранично: за один запрос можно получить до 250 записей, после чего производится переход на следующую страницу.  

Также были реализованы процедуры очистки данных: проверка наличия обязательных полей (`description`, `name`, `year`) и устранение дублирующихся записей (одна и та же картина может попадать в несколько жанров).

**Результаты сбора данных:**

- Общее количество записей: **102341**
- <details><summary>Количество по жанрам:</summary>
	
	   • аниме: 4213 фильмов
	   • биография: 5071 фильмов
	   • боевик: 10240 фильмов
	   • вестерн: 1764 фильмов
	   • военный: 4193 фильмов
	   • детектив: 9941 фильмов
	   • детский: 2094 фильмов
	   • для взрослых: 479 фильмов
	   • документальный: 6812 фильмов
	   • драма: 7211 фильмов
	   • игра: 113 фильмов
	   • история: 2132 фильмов
	   • комедия: 6729 фильмов
	   • концерт: 100 фильмов
	   • короткометражка: 4323 фильмов
	   • криминал: 4220 фильмов
	   • мелодрама: 5163 фильмов
	   • музыка: 1761 фильмов
	   • мультфильм: 2019 фильмов
	   • мюзикл: 1614 фильмов
	   • новости: 33 фильмов
	   • приключения: 3789 фильмов
	   • реальное ТВ: 509 фильмов
	   • семейный: 3657 фильмов
	   • спорт: 932 фильмов
	   • ток-шоу: 78 фильмов
	   • триллер: 3009 фильмов
	   • ужасы: 5589 фильмов
	   • фантастика: 2565 фильмов
	   • фильм-нуар: 153 фильмов
	   • фэнтези: 1813 фильмов
	   • церемония: 22 фильмов
	  
</details>

<details><summary>Пример данных: </summary>

```json
{
      "id": 7109663,
      "name": "Парадоксальный навык «Мастер фруктов»: Навык, позволяющий есть бесконечное число фруктов (правда, вы умрёте, лишь откусив их)",
      "alternativeName": "Hazure Skill «Kinomi Master»: Skill no Mi (Tabetara Shinu) wo Mugen ni Taberareru You ni Natta Ken ni Tsuite",
      "type": "anime",
      "typeNumber": 4,
      "year": 2024,
      "description": "Есть мир, где любой человек может получить особую способность, съев фрукт навыка. Но сделать это можно лишь один раз в жизни, а во второй раз обязательно умрёшь от отравления. \nЛайт Андервуд мечтал стать лучшим на свете авантюристом, однако ему, как назло, попался навык «Мастер фруктов» — совершенно не боевая способность, которая сгодится разве что сад выращивать. Его подруге детства Лене попался редкий и мощный навык «Святая меча», и её сразу же отправили в столицу и назначили авантюристкой S-ранга, а Лайт остался дома и начал заниматься фермерством. Однажды он случайно съедает второй фрукт навыка, но не умирает. Оказывается, его способность позволяет есть сколько угодно фруктов навыка. Так начинается его история успеха и путь к исполнению мечты.",
      "shortDescription": "Фермер узнает, что может мгновенно овладеть любым мастерством. Фэнтези-аниме о начинающем искателе приключений",
      "status": null,
      "rating": {
        "kp": 7.418,
        "imdb": 6.2,
        "filmCritics": 0,
        "russianFilmCritics": 0,
        "await": null
      },
      "votes": {
        "kp": 4069,
        "imdb": 198,
        "filmCritics": 0,
        "russianFilmCritics": 0,
        "await": 0
      },
      "movieLength": null,
      "totalSeriesLength": null,
      "seriesLength": 23,
      "ratingMpaa": null,
      "ageRating": 18,
      "poster": {
        "url": "https://image.openmoviedb.com/kinopoisk-images/4716873/fdd65c27-9937-4a71-b3d2-144098b3d80a/orig",
        "previewUrl": "https://image.openmoviedb.com/kinopoisk-images/4716873/fdd65c27-9937-4a71-b3d2-144098b3d80a/x1000"
      },
      "genres": [
        {
          "name": "аниме"
        },
        {
          "name": "мультфильм"
        },
        {
          "name": "фэнтези"
        },
        {
          "name": "боевик"
        },
        {
          "name": "приключения"
        }
      ],
      "countries": [
        {
          "name": "Япония"
        }
      ],
      "releaseYears": [
        {
          "start": 2024,
          "end": null
        }
      ],
      "top10": null,
      "top250": null,
      "isSeries": true,
      "ticketsOnSale": false,
      "backdrop": {
        "previewUrl": "https://image.openmoviedb.com/kinopoisk-ott-images/374297/2a00000194d147908e013bba964ea52f4012/x1000",
        "url": "https://image.openmoviedb.com/kinopoisk-ott-images/374297/2a00000194d147908e013bba964ea52f4012/orig"
      }
    }
```
</details>

## Выбор используемых технологий

- **API Кинопоиска через kinopoisk.dev** – для получения актуальных данных.
- **Python** – основной язык программирования.
- **Flask** – для разработки бекенд-части сайта, обеспечивающей поиск и рекомендации.
- **HTML/CSS/JavaScript** – для создания пользовательского интерфейса сайта.

## План реализации

1. **Сбор и очистка данных:**  
   - Выкачка данных по каждому жанру постранично.
   - Очистка данных от неполных записей и дубликатов.
   - Сохранение данных в формате JSON.

2. **Разработка бекенда на Flask:**  
   - Создание системы для поиска фильмов с фильтрацией по названию, жанрам, году выпуска и типу.
   - Создание системы для рекомендаций на основе выбранного фильма.

3. **Создание сайта:**  
   - Разработка удобного интерфейса поиска и отображения результатов.
   - Интеграция рекомендательной системы и фильтров.

## Разработка бекенд части

### Создание системы для поиска фильмов

Бекенд на Flask реализует следующие маршруты:
- Главная страница (`/`) с формой поиска.
- Страница результатов поиска (`/dml`), где выводятся найденные фильмы.
- API-эндпоинт `/search_movies` для AJAX-запросов поиска.
 
#### Опрос

Мы провели опрос с целью выяснения насколько удобно пользоваться нашим поисковиком и насколько правильно он подбирает фильмы.

94 участника ответили, что поиск работает `Хорошо`

15 участников ответили, что поиск требует доработки

![screenshot](https://github.com/user-attachments/assets/cd5b2ee3-b001-4266-b29e-e5b1fb544cca)
 ### Создание системы для рекомендации фильмов

Рекомендательная система реализована через отдельный эндпоинт `/recommend_movies`, который принимает параметры выбранного фильма (например, его ID и жанры) и возвращает список рекомендованных фильмов. 




## Создание сайта и пользовательский путь

Пользователь заходит на главную страницу, вводит название фильма в строку поиска, после чего происходит перенаправление на страницу с результатами (`/dml?query=...`).  
На странице результатов пользователь может дополнительно фильтровать фильмы по жанрам, году выпуска и типу, а при клике на карточку фильма открывается модальное окно с подробной информацией и списком рекомендованных фильмов.

---

