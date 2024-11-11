--  SQL Data Cleaning 

-- Project Aim: Clean the worldlayoffs dataset to prepare for EDA
-- Data Soucre: -- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Step 1: Preparing a copy of data for editing 
 
SELECT * 
FROM layoffs;

CREATE TABLE world_layoffs.layoffs_editable 
LIKE world_layoffs.layoffs; 

INSERT INTO layoffs_editable
	SELECT * 
    FROM world_layoffs.layoffs ;
    
SELECT * 
FROM layoffs_editable
;
-- Project steps 

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. find and coreect null/blank values if possible 
-- 4. remove any columns and rows that are not necessary


-- 1. Remove Duplicates

# First let's check for duplicates

-- Usual for finding duplicates across rows would be 
#SELECT 
    #col1, COUNT(col1),
    #col2, COUNT(col2),
    
#FROM table_name
#GROUP BY col1, col2, ...
#HAVING 
     #  (COUNT(col1) > 1) AND 
	 #  (COUNT(col2) > 1);

-- Code applied to layoffs_editable 
SELECT 
    company, COUNT(company),
    location,  COUNT(location),
    industry,  COUNT(industry),
    total_laid_off, COUNT(total_laid_off), 
    percentage_laid_off, COUNT(percentage_laid_off),
    `date`, COUNT(`date`),
    stage, COUNT(stage),
    country, COUNT(country),
    funds_raised_millions, COUNT(funds_raised_millions)
FROM layoffs_editable
GROUP BY 
	company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    `date`,
    stage,
    country,
    funds_raised_millions
HAVING  COUNT(company) > 1
    AND COUNT(location) > 1
    AND COUNT(industry) > 1
    AND COUNT(total_laid_off) > 1
    AND COUNT(percentage_laid_off) > 1
    AND COUNT(`date`) > 1
    AND COUNT(stage) > 1
    AND COUNT(country) > 1
    AND COUNT(funds_raised_millions) > 1
    ;
    
-- Double checking if the results really are duplicates 
SELECT *
FROM world_layoffs.layoffs_editable 
WHERE company IN ('Yahoo', 'Wildfire Studios', 'Cazoo', 'Hibob')
ORDER BY company
;

-- Problem: Dataset has no Unique row identifier making removal of duplicates diffcult 

-- Another method for finding duplicates using PARTITION BY make unique rows by grouping them by all column ab then using ROW_NUMBER to assign a number 

# I create subquey called duplicate records, in the outer query I chose where row_num > 1/ is a duplicate

SELECT *
FROM 
	( SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
	ROW_NUMBER() OVER (
	PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions ) AS row_num
	FROM 
		world_layoffs.layoffs_editable ) AS duplicates_records 
WHERE row_num > 1;

-- this duplicate method also includes the company Casper as a duplicate. Our earlier method, which counts, didn't find it because the duplicates have null values which can't be counted
SELECT *
FROM world_layoffs.layoffs_editable 
WHERE company = 'Casper'
;

-- We can rewrite it as a CTE 
WITH CTE AS
(SELECT *
FROM 
	( SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
	ROW_NUMBER() OVER (
	PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions ) AS row_num
	FROM 
		world_layoffs.layoffs_editable ) AS duplicates_records 
WHERE row_num > 1)

SELECT *
FROM CTE ;

--  But we can't delete the duplicates directly 


-- Create a new column Is to create a layoffs  table  and add those row numbers in. Then delete where row numbers are over 2
-- so let's do it!!

ALTER TABLE world_layoffs.layoffs_editable ADD row_num INT;

ALTER TABLE layoffs_editable
DROP COLUMN row_num;

CREATE TABLE `layoffs_editable2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert information from layoffs editable 

INSERT INTO layoffs_editable2
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
	ROW_NUMBER() OVER (
	PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions ) AS row_num
	FROM  world_layoffs.layoffs_editable ;

-- Now we just delete records with more than 1 row num
SELECT * 
FROM layoffs_editable2
WHERE row_num > 1;

DELETE 
FROM layoffs_editable2
WHERE row_num > 1;

-- 2. Standardize Data

# Trim Company column
SELECT * 
FROM layoffs_editable2;


SELECT company, TRIM(company)
FROM layoffs_editable2;
    

UPDATE layoffs_editable2
SET company = TRIM(company)
;

# Industry - contains blanks and multiple version of 'Cryto'
SELECT DISTINCT industry 
FROM layoffs_editable2
ORDER BY industry ASC
;


SELECT * 
FROM layoffs_editable2
WHERE industry LIKE 'Crypto%'
;
-- 
-- Let's update the layoffs table to make the name consistent

UPDATE layoffs_editable2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 



-- Checking other columns 

SELECT * 
FROM layoffs_editable2;


SELECT DISTINCT location
FROM layoffs_editable2
ORDER BY location ASC
;

SELECT DISTINCT country 
FROM layoffs_editable2
ORDER BY country ASC
;

-- country has two versions of US

UPDATE layoffs_editable2
SET country = 'United States'
WHERE country LIKE 'United States%'
;



-- Let's also fix the date columns - right now its a STR
SELECT date
FROM layoffs_editable2;

-- we can use str to date to update this field
UPDATE layoffs_editable2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_editable2
MODIFY COLUMN `date` DATE;


-- 3. Find and coreect null/blank values if possible 


-- if we look at industry it looks like we have some null and empty rows, let's take a look at these

SELECT DISTINCT industry
FROM world_layoffs.layoffs_editable2
ORDER BY industry;

--  the values with null or blank
SELECT *
FROM world_layoffs.layoffs_editable2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- we can look for the correct industry value if it is avaiable in other records e.g airbndb's industry is travel


SELECT *
FROM world_layoffs.layoffs_editable2
WHERE company LIKE 'Airbnb'
ORDER BY company;


UPDATE layoffs_editable2
SET industry = 'Travel'
WHERE company = 'Airbnb';


-- Join code to do it all in one go. If the table has a blank and non blank industry, replace the blank with the non-blank industry.

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_editable2
SET industry = NULL
WHERE industry = '';

-- with the code below we join the tables so we can find the industry information if it is avaliable 
SELECT *
FROM layoffs_editable2 AS le2_1
JOIN layoffs_editable2 AS le2_2
	ON le2_1.company = le2_2.company 
    -- making sure we only select those that are the same company
    WHERE le2_1.industry IS NULL AND le2_2.industry IS NOT NULL
    ;
    
UPDATE layoffs_editable2 le2_1
JOIN layoffs_editable2 le2_2
ON le2_1.company = le2_2.company
SET le2_1.industry = le2_2.industry
WHERE le2_1.industry IS NULL
AND le2_2.industry IS NOT NULL;


-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoffs_editable2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


-- 4. Remove any columns and rows that are not necessary
## Why? Increase querying speed and save storage memory

SELECT *
FROM layoffs_editable2;


-- We don't need the row_num column anymore

ALTER TABLE layoffs_editable2
DROP row_num;


-- where both total_laid_off and percentage laid off is probably un-usable information - I'll delete that
SELECT *
FROM layoffs_editable2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_editable2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Project End
-- Now our data is ready for EDA!