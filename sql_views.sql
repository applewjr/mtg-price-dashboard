-- price_after_launch
create or replace view PRICE_AFTER_LAUNCH(
    DATE_DIFF,
    AVG_USD,
    SET_NAME
) as

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    -- AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    AND static.released_at >= '2025-01-01'
    AND date_diff >= 1
    AND date_diff <= 300
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;


-- price_after_launch_rare_mythic
create or replace view PRICE_AFTER_LAUNCH_RARE_MYTHIC(
    DATE_DIFF,
    AVG_USD,
    SET_NAME
) as

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    AND static.released_at >= '2025-01-01'
    AND date_diff >= 1
    AND date_diff <= 300
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;


-- price_after_launch_foil
create or replace view PRICE_AFTER_LAUNCH_FOIL(
    DATE_DIFF,
    AVG_USD,
    SET_NAME
) as

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd_foil) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    -- AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    AND static.released_at >= '2025-01-01'
    AND date_diff >= 1
    AND date_diff <= 300
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;


-- price_after_launch_foil_rare_mythic
create or replace view PRICE_AFTER_LAUNCH_FOIL_RARE_MYTHIC(
    DATE_DIFF,
    AVG_USD,
    SET_NAME
) as

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd_foil) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    AND static.released_at >= '2025-01-01'
    AND date_diff >= 1
    AND date_diff <= 300
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;


-- price_before_launch
create or replace view PRICE_BEFORE_LAUNCH(
    DATE_DIFF,
    AVG_USD,
    SET_NAME
) as

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    -- AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    AND static.released_at >= '2025-01-01'
    AND date_diff >= -45
    AND date_diff <= 0
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;


-- price_before_launch_foil
create or replace view PRICE_BEFORE_LAUNCH_FOIL(
    DATE_DIFF,
    AVG_USD,
    SET_NAME
) as

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd_foil) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    -- AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    AND static.released_at >= '2025-01-01'
    AND date_diff >= -45
    AND date_diff <= 0
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;


