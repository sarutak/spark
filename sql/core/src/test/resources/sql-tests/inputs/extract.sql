-- extract fields from year-month/day-time intervals
select extract(YEAR from interval '2-1' YEAR TO MONTH);
select date_part('YEAR', interval '2-1' YEAR TO MONTH);
select extract(YEAR from -interval '2-1' YEAR TO MONTH);
select extract(MONTH from interval '2-1' YEAR TO MONTH);
select date_part('MONTH', interval '2-1' YEAR TO MONTH);
select extract(MONTH from -interval '2-1' YEAR TO MONTH);
select date_part(NULL, interval '2-1' YEAR TO MONTH);

-- invalid
select extract(DAY from interval '2-1' YEAR TO MONTH);
select date_part('DAY', interval '2-1' YEAR TO MONTH);
select date_part('not_supported', interval '2-1' YEAR TO MONTH);

select extract(DAY from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('DAY', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(DAY from -interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(HOUR from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('HOUR', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(HOUR from -interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(MINUTE from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('MINUTE', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(MINUTE from -interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(SECOND from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('SECOND', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(SECOND from -interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part(NULL, interval '123 12:34:56.789123123' DAY TO SECOND);

select extract(MONTH from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('not_supported', interval '123 12:34:56.789123123' DAY TO SECOND);
