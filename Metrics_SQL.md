## Анализ покупок пользователей в e-commerce

### Задачи

Необходимо проанализировать данные о сделанных заказах - посчитать метрики ARPU, ARPPU, AOV и другие.

### SQL анализ

Для каждого дня в таблице orders рассчитаем показатели:

- Выручку, полученную в этот день.
- Суммарную выручку на текущий день.
- Прирост выручки, полученной в этот день, относительно значения выручки за предыдущий день.
- Выручку на пользователя (ARPU) за текущий день.
- Выручку на платящего пользователя (ARPPU) за текущий день.
- Выручку с заказа, или средний чек (AOV) за текущий день.
- Накопленную выручку на пользователя (Running ARPU).
- Накопленную выручку на платящего пользователя (Running ARPPU).
- Накопленную выручку с заказа, или средний чек (Running AOV).

````sql
 SELECT date,
       revenue,
       total_revenue as total_reven,
        round(100*(revenue-lag(revenue, 1) OVER (ORDER BY date))/lag(revenue, 1) OVER (ORDER BY date)::decimal,
             2) as rev_change,
       round(revenue/count_user_d, 2) as arpu,
       round(revenue/count_user_pl_d, 2) as arppu,
       round(revenue/count_order, 2) as aov,
       round(total_revenue/count_user_::decimal, 2) as runn_arpu,
       round(total_revenue/count_user_pl_::decimal, 2) as runn_arppu,
       round(total_revenue/count_order_::decimal, 2) as runn_aov
FROM   (SELECT date,
                count_user_d,
                count_user_pl_d,
               count_order,
               sum (revenue) OVER (ORDER BY date) as total_revenue,
               sum (count_user) OVER (ORDER BY date) as count_user_,
               sum (count_user_pl) OVER (ORDER BY date) as count_user_pl_,
               sum (count_order) OVER (ORDER BY date) as count_order_,
               revenue
        FROM   (SELECT date(creation_time) as date,
                       sum(price) as revenue,
                       count(distinct order_id) as count_order
                FROM   (SELECT order_id,
                               creation_time,
                               unnest(product_ids) as product_id
                        FROM   orders
                        WHERE  order_id not in (SELECT order_id
                                                FROM   user_actions
                                                WHERE  action = 'cancel_order')) t1
                    LEFT JOIN products using(product_id)
                GROUP BY date) g
            LEFT JOIN (SELECT time::date as date,
                              count(distinct user_id) as count_user_d
                      FROM   user_actions
                      GROUP BY date) t5 using(date)
            LEFT JOIN (SELECT time::date as date,
                              count(distinct user_id) as count_user_pl_d
                       FROM   user_actions
                       WHERE  order_id not in (SELECT order_id
                                              FROM   user_actions
                                               WHERE  action = 'cancel_order')
                       GROUP BY date) t7 using(date)
                   
            LEFT JOIN (SELECT date,
                              count(user_id) as count_user
                       FROM   (SELECT min(time):: date as date,
                                      user_id
                               FROM   user_actions
                               GROUP BY user_id) t2
                       GROUP BY date) t4 using(date)
            LEFT JOIN (SELECT date,
                              count(distinct user_id) as count_user_pl
                       FROM   (SELECT min(time):: date as date,
                                      user_id
                               FROM   user_actions
                               WHERE  order_id not in (SELECT order_id
                                                       FROM   user_actions
                                                       WHERE  action = 'cancel_order')
                               GROUP BY user_id) t6
                       GROUP BY date) t3 using(date)) j
ORDER BY date
````
**Результат:**
| date       | revenue | total_reven | rev_change | arpu   | arppu  | aov    | runn_arpu | runn_arppu | runn_aov |
|------------|---------|-------------|------------|--------|--------|--------|-----------|------------|----------|
| 2022-08-24 | 49924   | 49924       |            | 372,57 | 393,1  | 361,77 | 372,57    | 393,1      | 361,77   |
| 2022-08-25 | 430860  | 480784      | 763,03     | 508,09 | 525,44 | 406,86 | 499,26    | 517,53     | 401,66   |
| 2022-08-26 | 534766  | 1015550     | 24,12      | 452,04 | 470,33 | 369,57 | 512,9     | 530,87     | 384,1    |
| 2022-08-27 | 817053  | 1832603     | 52,79      | 509,38 | 527,81 | 381,62 | 571,8     | 590,21     | 382,99   |
| 2022-08-28 | 1133370 | 2965973     | 38,71      | 528,38 | 544,1  | 378,04 | 632,13    | 649,72     | 381,08   |
| 2022-08-29 | 1279891 | 4245864     | 12,93      | 559,15 | 581,24 | 391,76 | 707,53    | 726,29     | 384,24   |
| 2022-08-30 | 1279377 | 5525241     | -0,04      | 546,74 | 567,85 | 379,52 | 766,86    | 786,4      | 383,14   |
| 2022-08-31 | 1312720 | 6837961     | 2,61       | 517,63 | 540,21 | 384,96 | 792,81    | 813,46     | 383,49   |
| 2022-09-01 | 1406101 | 8244062     | 7,11       | 499,33 | 518,86 | 381,26 | 813,18    | 832,9      | 383,11   |
| 2022-09-02 | 1907107 | 10151169    | 35,63      | 537,67 | 556,17 | 381,35 | 844,17    | 863,05     | 382,77   |
| 2022-09-03 | 2210988 | 12362157    | 15,93      | 565,9  | 582,76 | 387,28 | 886,24    | 904,39     | 383,57   |
| 2022-09-04 | 2294009 | 14656166    | 3,75       | 541,55 | 558,97 | 381,7  | 921,71    | 938,78     | 383,28   |
| 2022-09-05 | 1784690 | 16440856    | -22,2      | 512,84 | 530,84 | 381,75 | 950,45    | 967,17     | 383,11   |
| 2022-09-06 | 1330931 | 17771787    | -25,43     | 475,33 | 492,75 | 385,67 | 970,18    | 986,72     | 383,3    |
| 2022-09-07 | 1807800 | 19579587    | 35,83      | 496,65 | 514,02 | 378,44 | 992,38    | 1007,85    | 382,85   |
| 2022-09-08 | 2099508 | 21679095    | 16,14      | 521,1  | 536,68 | 383,54 | 1012,99   | 1028,03    | 382,91   |

##


- Выручку, полученную в этот день.
- Выручку с заказов новых пользователей, полученную в этот день.
- Долю выручки с заказов новых пользователей в общей выручке, полученной за этот день.
- Долю выручки с заказов остальных пользователей в общей выручке, полученной за этот день.

````sql
SELECT date,
       sum(revenue) as revenue,
       sum (revenue) filter (WHERE user_id > 0) as new_users_revenue,
       round(100*sum (revenue) filter (WHERE user_id > 0) / sum(revenue):: decimal,
             2) as new_users_revenue_share,
       100-round(100*sum (revenue) filter (WHERE user_id > 0) / sum(revenue):: decimal,
                 2) as old_users_revenue_share
FROM   (SELECT date,
               order_id,
               coalesce(user_id, 0) as user_id,
               revenue
        FROM   (SELECT creation_time::date as date,
                       order_id,
                       sum(price) as revenue
                FROM   (SELECT order_id,
                               creation_time,
                               unnest(product_ids) as product_id
                        FROM   orders
                        WHERE  order_id not in (SELECT order_id
                                                FROM   user_actions
                                                WHERE  action = 'cancel_order'))t1
                    LEFT JOIN products using(product_id)
                GROUP BY date, order_id) t2
            LEFT JOIN (SELECT date,
                              order_id,
                              user_id
                       FROM   (SELECT user_id ,
                                      min(time::date) as date
                               FROM   user_actions
                               GROUP BY user_id) t7
                           LEFT JOIN (SELECT user_id,
                                             order_id,
                                             (time::date) as date
                                      FROM   user_actions
                                      WHERE  order_id not in (SELECT order_id
                                                              FROM   user_actions
                                                              WHERE  action = 'cancel_order')) t8 using (user_id, date)) t9 using (order_id, date)) t3
GROUP BY date
````
**Результат:**
| date       | revenue | new_users_revenue | new_users_revenue_share | old_users_revenue_share |
|------------|---------|-------------------|-------------------------|-------------------------|
| 2022-08-24 | 49924   | 49924             | 100                     | 0                       |
| 2022-08-25 | 430860  | 417333            | 96,86                   | 3,14                    |
| 2022-08-26 | 534766  | 463326            | 86,64                   | 13,36                   |
| 2022-08-27 | 817053  | 619318            | 75,8                    | 24,2                    |
| 2022-08-28 | 1133370 | 801162            | 70,69                   | 29,31                   |
| 2022-08-29 | 1279891 | 717374            | 56,05                   | 43,95                   |
| 2022-08-30 | 1279377 | 656429            | 51,31                   | 48,69                   |
| 2022-08-31 | 1312720 | 720381            | 54,88                   | 45,12                   |
| 2022-09-01 | 1406101 | 757287            | 53,86                   | 46,14                   |
| 2022-09-02 | 1907107 | 1017824           | 53,37                   | 46,63                   |
| 2022-09-03 | 2210988 | 1079256           | 48,81                   | 51,19                   |
| 2022-09-04 | 2294009 | 1063997           | 46,38                   | 53,62                   |
| 2022-09-05 | 1784690 | 714459            | 40,03                   | 59,97                   |
| 2022-09-06 | 1330931 | 495058            | 37,2                    | 62,8                    |
| 2022-09-07 | 1807800 | 710154            | 39,28                   | 60,72                   |
| 2022-09-08 | 2099508 | 887959            | 42,29                   | 57,71                   |
