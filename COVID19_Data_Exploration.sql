---COVID-19 world data exploration---
---Created and imported data for tables: covid_deaths and covid_vaccinations each containing 85172 rows.
---Skills used: Joins, CTE's, Window functions, temp tables, subquerries, Aggregate function, creating views, converting data types.
 
CREATE TABLE covid_deaths (
       iso_code varchar,continent varchar,location varchar,date varchar,
	total_cases bigint,new_cases bigint,new_cases_smoothed varchar,
	total_deaths bigint,new_deaths bigint,new_deaths_smoothed varchar,
	total_cases_per_million varchar,new_cases_per_million varchar,
	new_cases_smoothed_per_million varchar,total_deaths_per_million varchar,
	new_deaths_per_million varchar,new_deaths_smoothed_per_million varchar,
	reproduction_rate varchar,icu_patients varchar,icu_patients_per_million varchar,
	hosp_patients bigint,hosp_patients_per_million varchar,weekly_icu_admissions varchar,
	weekly_icu_admissions_per_million varchar,weekly_hosp_admissions varchar,
	weekly_hosp_admissions_per_million varchar,new_tests bigint,total_tests bigint,
	total_tests_per_thousand varchar,new_tests_per_thousand varchar,
	new_tests_smoothed varchar,
	new_tests_smoothed_per_thousand varchar,positive_rate varchar,
	tests_per_case varchar,tests_units varchar,total_vaccinations bigint,people_vaccinated bigint,
	people_fully_vaccinated bigint,new_vaccinations bigint,new_vaccinations_smoothed varchar,
	total_vaccinations_per_hundred varchar,people_vaccinated_per_hundred varchar,
	people_fully_vaccinated_per_hundred varchar,new_vaccinations_smoothed_per_million varchar,
	stringency_index varchar,population bigint,population_density varchar,median_age varchar
	,aged_65_older varchar,aged_70_older varchar,gdp_per_capita varchar,
	extreme_poverty varchar,cardiovasc_death_rate varchar,diabetes_prevalence varchar,
	female_smokers varchar,male_smokers varchar,handwashing_facilities varchar,
	hospital_beds_per_thousand varchar,life_expectancy varchar,human_development_index varchar)
	
COPY covid_deaths FROM '/Users/nancy/Downloads/CovidDeaths.csv' WITH DELIMITER ',' CSV HEADER;

---------------------------------------------------------

CREATE TABLE covid_vaccinations (
	iso_code varchar,continent varchar,location varchar,date varchar,new_tests bigint,
	total_tests bigint,total_tests_per_thousand varchar,new_tests_per_thousand varchar,
	new_tests_smoothed varchar,new_tests_smoothed_per_thousand varchar,
	positive_rate varchar,tests_per_case varchar,tests_units varchar,total_vaccinations bigint,
	people_vaccinated bigint,people_fully_vaccinated bigint,new_vaccinations bigint,
	new_vaccinations_smoothed varchar,total_vaccinations_per_hundred varchar,
	people_vaccinated_per_hundred varchar,people_fully_vaccinated_per_hundred varchar,
	new_vaccinations_smoothed_per_million varchar,stringency_index varchar,
	population_density varchar,median_age varchar,aged_65_older varchar,
	aged_70_older varchar,gdp_per_capita varchar,extreme_poverty varchar,
	cardiovasc_death_rate varchar,diabetes_prevalence varchar,
	female_smokers varchar,male_smokers varchar,handwashing_facilities varchar,
	hospital_beds_per_thousand varchar,life_expectancy varchar,human_development_index varchar)

COPY covid_vaccinations FROM '/Users/nancy/Downloads/CovidVaccinations.csv' WITH DELIMITER ',' CSV HEADER;

------------------------------------
--Setting datestyle to avoid the error---

SET datestyle = 'ISO, DMY';

-- Starting with data exploration--

Select Location, cast(date as TIMESTAMP), total_cases, new_cases, total_deaths, population
From covid_deaths
Where continent is not null 
order by 1,2

---Maximum deaths per continent

SELECT	location as continent, MAX(total_deaths) as TotalDeathCount
FROM covid_deaths
WHERE	continent is null and total_deaths IS NOT NULL
GROUP BY	location
ORDER BY	2 DESC

---
--death count country wise

SELECT	location, MAX(total_deaths) as TotalDeathCount
FROM		covid_deaths
WHERE		total_deaths IS NOT NULL and continent IS NOT NULL 
GROUP BY	location
ORDER BY	2 DESC;

--

select location, total_deaths
FROM covid_deaths
WHERE	total_deaths IS NOT NULL 


---BREAKING DOWN BY Country
SELECT		location, MAX(total_cases)as total_infection
FROM		covid_deaths
WHERE		continent is not null and total_cases is not null
GROUP BY	location
ORDER BY	2 desc;
---showing the continent with highest death count
SELECT		location, MAX(total_deaths) as total_death_count
FROM		covid_deaths
WHERE		continent IS NULL
GROUP BY	location
ORDER BY	2 DESC;

---BREAKING GLOBAL NUMBERS

SELECT		date::date,location,SUM(new_cases) as case_number
FROM		covid_deaths 
WHERE		continent is not null and new_cases is not null
GROUP BY	date, location
ORDER BY	1,2

--GLOBAL DEATHS

SELECT		SUM(new_cases)as total_cases,SUM(new_deaths) as total_deaths,
			SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
FROM		covid_deaths
WHERE		continent IS NOT NULL
-- GROUP BY	date
ORDER BY	1,2

--Looking at total population v/s vaccinations

SELECT		dea.continent, dea.location, dea.date::date, dea.population,
			vac.new_vaccinations, 
			SUM(vac.new_vaccinations) OVER (Partition By dea.location ORDER BY dea.date )
			as rolling_people_vaccinated
FROM		covid_deaths dea
			JOIN covid_vaccinations vac
			ON dea.location = vac.location
			and dea.date = vac.date
WHERE		dea.continent is not null and vac.new_vaccinations is not null
ORDER BY	2,3

--USED CTE to find the vaccination percentage  

WITH pop_vs_vac (continent,location, date, population, new_vaccinations, rolling_people_vaccinated) as
(
SELECT		dea.continent, dea.location, dea.date::date, dea.population,
			vac.new_vaccinations, 
			SUM(vac.new_vaccinations) OVER (Partition By dea.location ORDER BY dea.date )
			as rolling_people_vaccinated
FROM		covid_deaths dea
			JOIN covid_vaccinations vac
			ON dea.location = vac.location
			and dea.date = vac.date
WHERE		dea.continent is not null and vac.new_vaccinations is not null
ORDER BY	dea.location, dea.date
)
SELECT		*, ROUND((rolling_people_vaccinated/population)*100 ::Numeric,2) as vaccination_percentage
FROM		pop_vs_vac

----
---Creating view to store for later visualization

CREATE VIEW percent_population_vaccinated as
SELECT		dea.continent, dea.location, dea.date, dea.population,
			vac.new_vaccinations, 
			SUM(vac.new_vaccinations) OVER (Partition By dea.location ORDER BY dea.date )
			as rolling_people_vaccinated
FROM		covid_deaths dea
			JOIN covid_vaccinations vac
			ON dea.location = vac.location
			and dea.date = vac.date
WHERE		dea.continent is not null and vac.new_vaccinations is not null
-- ORDER BY	dea.location, dea.date

DROP view percent_population_vaccinated

SELECT *
FROM percent_population_vaccinated

--Total cases v/s total deaths
--Shows likelihood of dying if contracted COVID in respective countries.
SELECT	location, population,
		SUM(total_deaths) as total_deaths, 
		SUM(total_cases) as total_cases,
		ROUND((SUM(total_deaths)/SUM(total_cases)*100)::NUMERIC,2)		
FROM	covid_deaths
WHERE	continent is not null
		AND total_cases is not null
		AND total_deaths is not null
GROUP BY	location,population
ORDER BY	location


--5 counties with highest cases of COVID 
SELECT	location, MAX(total_cases) as highest_cases 
-- 		MAX(total_deaths) as highest_deaths
FROM	covid_deaths
WHERE	continent is not null and total_cases is not null
GROUP BY	location
ORDER BY	2 DESC
LIMIT		5;

--5 countries with highest death rate
SELECT	location,
		MAX(total_deaths) as highest_deaths
FROM	covid_deaths
WHERE	continent is not null and total_deaths is not null
GROUP BY	location
ORDER BY	2 DESC
LIMIT	5;

--Pop vs COVID-19 case
SELECT	location, date, population,
		total_cases,
		(total_cases::numeric/population::numeric)*100 as infection_rate
FROM	covid_deaths
WHERE	continent is not null		
-- GROUP BY	location,population
ORDER BY	1,2 DESC

