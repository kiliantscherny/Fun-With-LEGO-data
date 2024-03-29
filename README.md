```
   ___                                       __     __                    
 /'___\                                  __ /\ \__ /\ \                   
/\ \__/  __  __    ___       __  __  __ /\_\\ \ ,_\\ \ \___               
\ \ ,__\/\ \/\ \ /' _ `\    /\ \/\ \/\ \\/\ \\ \ \/ \ \  _ `\             
 \ \ \_/\ \ \_\ \/\ \/\ \   \ \ \_/ \_/ \\ \ \\ \ \_ \ \ \ \ \            
  \ \_\  \ \____/\ \_\ \_\   \ \___x___/' \ \_\\ \__\ \ \_\ \_\           
   \/_/   \/___/  \/_/\/_/    \/__//__/    \/_/ \/__/  \/_/\/_/           
 ___                                     __             __                
/\_ \                                   /\ \           /\ \__             
\//\ \       __      __      ___        \_\ \      __  \ \ ,_\     __     
  \ \ \    /'__`\  /'_ `\   / __`\      /'_` \   /'__`\ \ \ \/   /'__`\   
   \_\ \_ /\  __/ /\ \L\ \ /\ \L\ \    /\ \L\ \ /\ \L\.\_\ \ \_ /\ \L\.\_ 
   /\____\\ \____\\ \____ \\ \____/    \ \___,_\\ \__/.\_\\ \__\\ \__/.\_\
   \/____/ \/____/ \/___L\ \\/___/      \/__,_ / \/__/\/_/ \/__/ \/__/\/_/
                     /\____/                                              
                     \_/__/                                               
```

<h1 align="center">Fun with LEGO data</h1>
<p align="center">Data Talks Club Data Engineering ZoomCamp final project <img src="images/Lego-Bricks-Stacked.png" alt="Stacked Lego Bricks" height="15" width="15"> by <a href="https://www.linkedin.com/in/kiliantscherny/"> Kilian Tscherny</a></p>

## Quickstart
>[!TIP]
>Want to dive right in and see the dashboard? Here's the link: [Looker Studio dashboard](https://lookerstudio.google.com/reporting/77580abe-d941-4b1c-9a73-61b0b34bcb6c).
>
>Want to run this locally? Skip to that part here: [How to reproduce this project locally](#how-to-reproduce-this-project-locally).

## Introduction
LEGO is one of my favourite toys in the world. Growing up, I was always spending much of my free time building sets or new creations, which to this day I still really enjoy. When it came to choosing a project for this course, LEGO was almost a no-brainer.

Even better, there is a large LEGO fan community online and plenty of interesting data about the many products LEGO has released over the years. This project aims to explore some of this data and build a dashboard to visualize it.

## Data
In this project I'm exploring a few different sources of data:
1. Rebrickable's [database](https://rebrickable.com/downloads/) of LEGO sets, parts, themes and more – updated daily and published as CSVs
2. Brick Insights, which offers a [Data Export](https://brickinsights.com/api/sets/export) via an API on interesting information like price, rating, number of reviews and more
3. Some LEGO data from researchers at the Gdańsk University of Technology, which includes information about LEGO sets release dates and retail prices
4. LEGO's own website, which can be scraped for additional information (note: the use of this was ultimately not included in the project, but I have kept any code related to this in this repository)

### Rebrickable
There are 12 different tables in the Rebrickable database:
| Table | Description |
| --- | --- |
| `themes` | Every LEGO theme – e.g. Star Wars, Racers |
| `colors` | Every colour a LEGO part has been released in |
| `part_categories` | Information about the types of parts – e.g. Plates, Technic Bricks |
| `parts` | The name, number and material of each part |
| `part_relationships` | Details about the relationships between different parts |
| `elements` | The combination of a part and its colour is an element |
| `sets` | Every LEGO set, with set number and name – e.g. 10026-1 Naboo Fighter |
| `minifigs` | LEGO minifigures with name and number |
| `inventories` | Inventories can contain parts, sets and minifigs |
| `inventory_parts` | The parts contained within an inventory |
| `inventory_sets` | The sets contained within an inventory |
| `inventory_minifigs` | The minifigs contained within an inventory |


**See the ERD here to understand the relationships between the tables:**

![Rebrickable ERD](images/rebrickable_erd.webp)

These are available for direct download as compressed CSV files (`.csv.gz`).

### Brick Insights
Brick Insights offers a Data Export via an API. The data is available in JSON format and can be accessed via a request to `https://brickinsights.com/api/sets/{set_id}`. The returned data includes interesting information like rating, review website, number of reviews and more.

```json
{
  "id": 10430,
  "set_id": "7078-1",
  "name": "King's Battle Chariot",
  "year": "2009",
  "image": "sets/7078-1.jpg",
  "average_rating": "71",
  "review_count": "5",
  "url": "https://brickinsights.com/sets/7078-1",
  "image_urls": {
    "small": "/crop/small/sets/7078-1.jpg",
    "medium": "/crop/medium/sets/7078-1.jpg",
    "large": "/crop/large/sets/7078-1.jpg",
    "teaser": "/crop/teaser/sets/7078-1.jpg",
    "teaser_compact": "/crop/teaser_compact/sets/7078-1.jpg",
    "generic140xn": "/crop/generic140xn/sets/7078-1.jpg",
    "generic200xn": "/crop/generic200xn/sets/7078-1.jpg",
    "genericnx400": "/crop/genericnx400/sets/7078-1.jpg",
    "original": "/crop/original/sets/7078-1.jpg"
  },
  "reviews": [
    {
      "review_url": "https://www.eurobricks.com/forum/index.php?/forums/topic/29660-review-7078-kings-battle-chariot/&do=findComment&comment=511525",
      "snippet": "The best thing about this set, hands-down, is the minifigures. They are beautiful. The accessories for them are also really great, a big plus for this set. The color scheme and parts selection is quite good, but it is at least 50 pieces under what is today the standard of price-per-piece. There are also a bunch of flaws in the design, as I demonstrated above.",
      "review_amount": "None",
      "rating_original": "8.8",
      "rating_converted": "88.00",
      "author_name": "Clone OPatra",
      "embed": "None"
    },
    {
      "review_url": "https://brickset.com/reviews/set-7078-1",
      "snippet": "None",
      "review_amount": "13",
      "rating_original": "3.9",
      "rating_converted": "78.00",
      "author_name": "None",
      "embed": "None"
    },
    {
      "review_url": "https://www.amazon.com/LEGO-Castle-Kings-Battle-Chariot/dp/B001US06LO?SubscriptionId=AKIAIGQAU2PVYTRCS5MQ&tag=brickinsights-20&linkCode=xm2&camp=2025&creative=165953&creativeASIN=B001US06LO",
      "snippet": "This is a fun set if you want to get the king out of seige castle and onto the battle field!!Send the king into battle agianst the trolls! or whoever you choose whahaahha.Nice chariot rolls very well. You get a horse with battle helmet!! always cool you also get a knight and treasure chest!! and a troll minifig to run over in you chariot!!Fun set, can be used for battle or treasure transportation. Very versitile and fun!",
      "review_amount": "16",
      "rating_original": "3.6",
      "rating_converted": "72.00",
      "author_name": "None",
      "embed": "None"
    },
    {
      "review_url": "http://guide.lugnet.com/set/7078",
      "snippet": "None",
      "review_amount": "11",
      "rating_original": "67",
      "rating_converted": "67.00",
      "author_name": "None",
      "embed": "None"
    },
    {
      "review_url": "https://www.eurobricks.com/forum/index.php?/forums/topic/29471-review-7078-king%E2%80%99s-battle-chariot/&do=findComment&comment=508063",
      "snippet": "It's sad that this set saw the light of day, it seems like something we'd have seen in KKII rather than the much better designed castle range that we have at the moment.",
      "review_amount": "None",
      "rating_original": "26",
      "rating_converted": "52.00",
      "author_name": "Cardinal Brick",
      "embed": "None"
    }
  ]
}
```

A set can have multiple reviews from different websites, each with a rating and a review snippet. This enables us to later analyze the average rating, the range within the ratings of a given set, the average number of reviews, among many other things.

Note that there is a large number of reviews available, and getting all of them is time-consuming (it took my script _11 hours 30 minutes_ to try to get all the reviews for all of the 22,485 sets available at the time of writing). For this reason, when replicating this, you may want to limit the number of years for which you want to get the data.

### LEGO data from Gdańsk University of Technology[^1] (henceforth "Aggregated Data")

From [the documentation](https://mostwiedzy.pl/en/open-research-data/data-on-lego-sets-release-dates-and-retail-prices-combined-with-aftermarket-transaction-prices-betwe,10210741381038465-0):
> The dataset contains LEGO bricks sets item count and pricing history for AI-based set pricing prediction.
>
> The data was downloaded from three sources. LEGO sets retail prices, release dates, and IDs were downloaded from Brickset.com, where one can find the ID number of each set that was released by Lego and its retail prices. The current status of the sets was downloaded from Lego.com and the retail prices for Poland and prices from aftermarket transactions were downloaded from promoklocki.pl. The data was merged based on the official LEGO set ID.

I'm using this dataset primarily for the set price data, which offers some unique insights opportunities. Huge thank you to the authors for making this data available for reuse.


[^1]: Oczkoś, W., Podgórski, B., Szczepańska, W., & Boiński, T. M. (2024). Data on LEGO sets release dates and worldwide retail prices combined with aftermarket transaction prices in Poland between June 2018 and June 2023. Data in Brief, 52, 110056. https://doi.org/10.1016/j.dib.2024.110056



### LEGO's website
LEGO's [website](https://www.lego.com/en-us) offers a lot of information about their products. This includes general product information, set rating, the price and more. This information can be scraped from the website using a library like BeautifulSoup.

> [!WARNING]  
> Scraping the LEGO website is at your own risk and discretion. Your IP address might also be blocked if you scrape too much data in too short a time. Please be careful and considerate when scraping websites.

Scraping the LEGO website is a time-consuming process and can be difficult to do for a large number of pages. For this project, I'm focusing on the Rebrickable, Brick Insights and the Aggregated data, but you can explore this data source further if you wish. I have created a script to do this, but its use is not necessary for this project.

## Project

### [The brief](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/projects#problem-statement)

>**Develop a dashboard with two tiles by:**
>
>- Selecting a dataset of interest (see Datasets)
>- Creating a pipeline for processing this dataset and putting it to a datalake
>- Creating a pipeline for moving the data from the lake to a data warehouse
>- Transforming the data in the data warehouse: prepare it for the dashboard
>- Building a dashboard to visualize the data

### Problem area
There are thousands of LEGO sets that have been released over the years, spanning many different themes and sizes. But how does the LEGO community feel about these sets and how do the prices of these sets also come into play? It would be super interesting to understand not only the in-depth details about the contents of each LEGO set, but also their popularity, average retail price, and much more.

It's a complex thing to try to solve, but this is my attempt at just that.

In this project, I'm primarily interested in answering questions around sets, prices, ratings and reviews. However, due to the large amount of data available, there are many other questions that could be answered with this data – I'm just going to tackle a few, but feel free to explore more!

### My approach: how it works

1. **Extract (`E`): Airflow & Python scripts for batch processing**
   - Download the complete Rebrickable database
   - Use the Brick Insights API to get reviews and ratings information about each set
   - Download the aggregated data file from the GUT website
   - [Optional] scrape LEGO's website for more information
2. **Load (`L`): GCP Cloud Storage and BigQuery**
   - Convert all files to Parquet format
   - Store the extracted and converted data file in a data lake (GCP Bucket)
   - Load the data into a data warehouse by creating BigQuery External tables pointing to the Parquet files in the GCS Bucket
4. **Transform (`T`): dbt Cloud**
   - Create a data pipeline for processing the different data sources
       - Use `staging` (basic cleaning, `view` materialization), `intermediate` (transformation, `ephemeral` materialization), and `marts` (final aggregation & presentation, `table` materialization)
   - Clean, transform, test and document the data to increase its usefulness for analytics
       - Cast columns to the correct data types, document all fields, add appropriate testing to check assumptions
5. **Visualize (`V`): Looker Studio**
   - Connect Looker Studio to the BigQuery project
   - Build a dashboard to visualize the data
  
### Orchestration and scheduling

Since this project is orchestrated with Airflow, you have the option to run each DAG once, or set them to run on a schedule to batch process the data from each source.

- The Rebrickable database is constantly updated, so this makes sense to continuously update if you want the freshest data.
- The Brick Insights API seems to be regularly updated as well, so you may wish to keep this DAG running on a schedule too.
- The aggregated LEGO data from GUT is a single file that isn't being updated, so in this case you should forgo scheduling this DAG to run more than once.
- Optionally, should you wish to scrape the LEGO website, this is something you will probably want to schedule to run frequently as new sets get added or the page information gets updated.


![Data Flow Diagram](images/data_flow_diagram.png)

## How to reproduce this project locally

### Prerequisites
> [!CAUTION]
> **You need *all* of the following to recreate this project locally**
- A [Google Cloud](https://cloud.google.com/) account with the necessary permissions and APIs enabled (e.g. service accounts, plus the BigQuery and Cloud Storage APIs – all instructions on how to do this are covered in the [DTC course](https://github.com/DataTalksClub/data-engineering-zoomcamp))
- [Python](https://www.python.org/downloads/) installed on your machine
- [Terraform](https://developer.hashicorp.com/terraform/install) installed on your machine
- [Docker](https://docs.docker.com/engine/install/) installed on your machine
- A [dbt Cloud](https://www.getdbt.com/product/dbt-cloud) account (it's possible to do this with dbt Core, but this is not covered in this project)

### Steps

0. **Authenticate with the `gcloud` CLI**
    ```bash
    $ gcloud auth application-default login
    ```
    Follow the instructions in your browser to authenticate with your Google Cloud account.

1. **Clone the repository to your machine**
   ```git
   $ gh repo clone kiliantscherny/Fun-With-LEGO-data
   ```
2. **Terraform: to set up the infrastructure (GCP Cloud Storage & BigQuery)**
>[!IMPORTANT]
>You will need to set some variables in the `/infrastructure/variables.tf` file to match your GCP project ID, the name of the BigQuery dataset you want to create and the name of the bucket you want to create in Cloud Storage. You can also change the region and location if you want to.
   - `cd` to the `/infrastructure/` folder
   - Update the following variables in the `variables.tf` file:
       - `project_id` – your GCP project ID
       - `bq_dataset_name` – the name of the BigQuery dataset you want to create where the raw data will be loaded
       - `bucket_name` – the name of the bucket you want to create in Cloud Storage
       - [optionally] the `region` and `location` if you want to change the default values
   - Run `terraform init`, `terraform plan` and `terraform apply` to create the necessary infrastructure
3. **Airflow: to set up the DAGs that Extract and Load the data**
>[!IMPORTANT]
> You will need to set your GCP project ID, the name of the BigQuery dataset, and the name of the bucket (which you created in the previous step) in the `/airflow/.env` file. The names must match exactly what you set in the Terraform step.
   - `cd` to the `/airflow/` folder
   - Update the following variables in the `docker-compose.yaml` file:
       - `GCP_PROJECT_ID` – your GCP project ID (the same as in the Terraform step)
       - `GCP_GCS_BUCKET` – the name of the bucket you created in Cloud Storage (also the same as in the Terraform step)
   - Run `docker-compose build` to build the image from the `Dockerfile`, then run `docker-compose up airflow-init`, followed by `docker-compose up -d` to start Airflow
   - Open `localhost:8080` in your browser and run the DAGs you want to extract and load the data
   - If the webserver isn't responding after a while or the container is unhealthy (after checking with `docker ps`), you might just need to restart the container with `docker-compose restart airflow-webserver`. If that doesn't work, try shutting down all the containers with `docker-compose down --volumes --rmi all` and re-running `docker-compose up -d`
   - [optionally] feel free to update the start and end dates (as well as the crontab) of each DAG to suit your needs if you wish to extract and load the data at different times
   - Choose which DAG(s) to run:
     - If you want to ingest the Rebrickable database, run the `REBRICKABLE_DATA_INGESTION` DAG
     - If you want to get the Brick Insights data, run the `BRICK_INSIGHTS_LEGO_SET_RATINGS` DAG
     - If you want to ingest the Aggregated LEGO data, run the `AGGREGATED_LEGO_DATA_INGESTION` DAG
     - If you want to scrape the LEGO website (optional), run the `LEGO_WEBSITE_SET_PRICES_RATINGS` DAG
>[!TIP]
>You will need to run `REBRICKABLE_DATA_INGESTION` first, before you can run any of the other DAGs, as they are reliant on the `sets` table that is created from it
   - Depending on several variables (the number years for which you want set information, your internet connection, etc.), the DAGs can take a while to run (from a few minutes to several hours)
   - To stop Airflow, run `docker-compose down --volumes --rmi all`
4. **dbt Cloud: to transform your raw data into something useful**
   - As mentioned above, you will need a dbt Cloud account to do this, but it's perfectly possible to do this with dbt Core (not covered in these instructions, but there are plenty of resources online to help you with this)
   - Connect your BigQuery project to dbt Cloud by following [the guide](https://docs.getdbt.com/guides/bigquery?step=1)
   - When everything is connected, you can execute `dbt build` to run and test the dbt models
   - The transformed data will be stored in a new dataset in BigQuery
5. **Connect your Looker Studio instance to your BigQuery project**
   - Connect your BigQuery project to Looker Studio by following [the guide](https://support.google.com/looker-studio/answer/6370296?hl=en#zippy=%2Cin-this-article)
   - Connect your Looker Studio instance to the production run dataset(s) in BigQuery
   - You'll be able to generate insights based on the transformed data in BigQuery

## [Dashboard](https://lookerstudio.google.com/reporting/77580abe-d941-4b1c-9a73-61b0b34bcb6c)

Follow the link above or click the image below to go to the Looker Studio Dashboard.

[![](images/Fun_with_LEGO_data.jpg)](https://lookerstudio.google.com/reporting/77580abe-d941-4b1c-9a73-61b0b34bcb6c)

## Contact

If you have any questions or feedback, feel free to reach out to me on [LinkedIn](https://www.linkedin.com/in/kiliantscherny/) or through the [DTC Slack community](https://datatalks-club.slack.com/team/U05ADPP9KFF)!
