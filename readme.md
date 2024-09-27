# Goodsale-Python-Test-Task
Сервис для обработки XML-выгрузки товаров из маркетплейса. 
Загружает данные в PostgreSQL, интегрируется с Elasticsearch для поиска похожих товаров. 
Итеративно обрабатывает  большие файлы с использованием lxml. Проект включает Docker-контейнеры, поддержку PEP-8 и файл конфигурации для линтинга.

Пример найденных похожих товаров:
Товар Чехол на Samsung Galaxy A3 2017 / Самсунг Галакси А3 2017 с принтом Синий космос 3879e17b-d7ef-4b1c-94c5-9806cc13fff1 похож на:
 - 0c360a6e-a9bc-4aff-a2b0-e35993829be5: Чехол на Samsung Galaxy A3 2017 / Самсунг Галакси А3 2017 с принтом Бриллианты
 - 454f9783-77ea-4c61-bf71-a041bc1adca1: Чехол на Samsung Galaxy A3 2017 / Самсунг Галакси А3 2017 с принтом IBM

Товар Телескоп Sky-Watcher SKYMAX BK MAK90EQ1, настольный f821c98e-c152-4f8c-b6b2-107b33af9e21 похож на:
- 6a363a15-3027-4186-88f7-239cb50aefcb: Телескоп Sky-Watcher SKYMAX BK MAK90EQ1, настольный
- 8323b79a-e578-4bde-8b56-43057bb155dc: Телескоп Orion GoScope III 70mm (рефрактор на фотоштативе в комплекте с рюкзаком)


Дополнительно заполнены поля category_lvl_1, category_lvl_2, category_lvl_3, category_remaining на основании парсинга всех существующих категорий.

Из формата
 <categories>
   <category id="90401">Все товары</category>
   <category id="198119" parentId="90401">Электроника</category>
   <category id="4976480" parentId="198119">Оптические приборы</category>
   <category id="10599873" parentId="198119">Аудио- и видеотехника</category>
   <category id="1558993" parentId="198119">Игровые приставки и аксессуары</category>
   <category id="944108" parentId="198119">Портативная техника</category>
   ...
</categories>
для каждого товара построен его иерархический путь, которы приведен к требуемому формату:
"category_lvl_1,   category_lvl_2,   category_lvl_3,   category_remaining"


