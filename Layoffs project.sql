select * from layoffs; 


-- Data Cleaning Agenda
------ 1. Removing duplicates 
------ 2. Identifying NULL values or blank values
------ 3. Remove any unwanted columns
------ 4. Standardize the data ;

describe layoffs;


-- created duplicate table for having raw data backup.

create table layoffs_duplicate like layoffs;
insert into layoffs_duplicate select * from layoffs;
select * from layoffs_duplicate;

-- - 1. Remove Duplicates 


with duplicate_CTE as 
( SELECT *, 
       ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, `date` ORDER BY company) AS row_num
FROM layoffs_duplicate)
select * from duplicate_CTE where row_num>1 ;

-- there are few duplicates but let me confirm 

select * from layoffs_duplicate where location = 'Oslo';

--- looks like they are valid entries not to be deleted.

with duplicate_1 as ( select * , row_number() OVER 
( Partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) 
AS row_num from layoffs_duplicate) select * from duplicate_1 where row_num > 1;

select * from layoffs_duplicate where location = 'London' and company = 'Cazoo';

-- created new table 

CREATE TABLE `layoffs_duplicate2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


Insert into layoffs_duplicate2
select * , row_number() OVER 
( Partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) 
AS row_num from layoffs_duplicate;

select * from layoffs_duplicate2 where row_num > 1;

--- delete after checking 

delete from layoffs_duplicate2 where row_num > 1;

-- 2. Standardizing Data

update layoffs_duplicate2
set company = trim(company);

select * from layoffs_duplicate2 where industry like 'Crypto%';

update layoffs_duplicate2 
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country from layoffs_duplicate2 order by 1;

UPDATE layoffs_duplicate2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


--- update the date format from text to date format

select `date`, str_to_date(`date`, '%m/%d/%Y') from layoffs_duplicate2;

update layoffs_duplicate2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date` from layoffs_duplicate2;

-- still date column data type is text converting it into date format by alter table

alter table layoffs_duplicate2
modify column `date` date;

---- removed unwanted data and columns which are not used.

select * from layoffs_duplicate2;

select * from layoffs_duplicate2
where total_laid_off is null and percentage_laid_off is NULL;

delete from layoffs_duplicate2
where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_duplicate2
drop column row_num;


select distinct industry from layoffs_duplicate2;

-- there are some null and ' ' rows in industry

select * from layoffs_duplicate2
where industry is null or industry = '';

select * from layoffs_duplicate2 where company = 'Airbnb';

-- set blanks to null values in industry

update layoffs_duplicate2
set industry = NUll
where industry = '';

-- now populate null values with the related industry 

select t1.industry,t2.industry from layoffs_duplicate2 t1 join layoffs_duplicate2 as t2
on t1.company = t2.company where t1.industry is null and t2.industry is not null;

update layoffs_duplicate2 t1 
join layoffs_duplicate2 t2
on t1.company = t2.company 
set t1.industry = t2.industry 
where t1.industry is null and t2.industry is not null;


select distinct industry from layoffs_duplicate2;

select * from layoffs_duplicate2;

-- Now, this is Cleaned Dataset


-------------------------------------------------------------------------------------------------------------------------

-- Part 2

--- Exploratory Data Analysis 

select max(total_laid_off), max(percentage_laid_off) from layoffs_duplicate2;

select * from layoffs_duplicate2 where percentage_laid_off = 1 order by funds_raised_millions desc;

-- companies with most total laid offs 

select company, sum(total_laid_off) from layoffs_duplicate2 group by company order by 2 desc;

-- which year most people laid off

select year(`date`) as a, sum(total_laid_off) from layoffs_duplicate2
group by a order by 1 desc;

-- companies with biggest single layoffs

select company, total_laid_off
from layoffs_duplicate2 order by 2 desc limit 5;

select company, sum(total_laid_off)
from layoffs_duplicate2 
group by company
order by 2 desc limit 5;

-- companies with most layoffs per year

with company_year as 
( select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
from layoffs_duplicate2 group by company, year(`date`) 
) , company_year_rank as 
( select company, years, total_laid_off, 
dense_rank() over (partition by years order by total_laid_off desc) as ranking from company_year )
select company, years, total_laid_off, ranking from company_year_rank where years is not null and ranking <= 5
order by years asc, total_laid_off desc;

--- Rolling Total of layoffs per month 

select substring(`date`,6,2) as MNTH, sum(total_laid_off) as total_laid_off 
from layoffs_duplicate2 group by MNTH 
order by MNTH asc;

with MNTH_CTE as
( select substring(`date`,1,7) as MNTH, sum(total_laid_off) as total_laid_off from layoffs_duplicate2
group by MNTH order by MNTH asc ) 
select MNTH, total_laid_off, sum(total_laid_off) over(order by MNTH ASC ) as rolling_total 
from MNTH_CTE where MNTH is not null order by MNTH asc
;






