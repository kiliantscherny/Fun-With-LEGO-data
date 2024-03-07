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
<p align="center">Data Talks Club Data Engineering ZoomCamp final project <img src="images/Lego-Bricks-Stacked.png" alt="Stacked Lego Bricks" height="15" width="15"> by Kilian Tscherny</p>

## Introduction
LEGO is one of my favourite things in the world. I've been playing with it since I was a child and I still enjoy it today. I've always been fascinated by the endless possibilities of building something new and unique with just a few bricks. I also love the idea of reusing and repurposing old bricks to create something new. This is why I chose to work with LEGO data for my final project in the Data Talks Club Data Engineering ZoomCamp.

Even better, there is a large LEGO fan community online and plenty of interesting data about the many products LEGO has released over the years.

## Data
In this project I'm exploring a few different sources of data:
1. Rebrickable's [database](https://rebrickable.com/downloads/) of LEGO sets, parts, themes and more – updated daily and published as CSVs
2. Brick Insights, which offers a [Data Export](https://brickinsights.com/api/sets/export) via an API on interesting information like price, rating, number of reviews and more
3. LEGO's own website, which can be scraped for additional information

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
Brick Insights offers a Data Export via an API. The data is available in JSON format and can be accessed via a request to `https://brickinsights.com/api/sets/{set_id}`. The returned data includes interesting information like price, rating, number of reviews and more.

```json
{
   "id":19199,
   "set_id":"75575-1",
   "name":"Ilu Discovery",
   "year":"2023",
   "image":"sets/75575-1.jpg",
   "average_rating":"None",
   "review_count":"1",
   "url":"https://brickinsights.com/sets/75575-1",
   "image_urls":{
      "small":"/crop/small/sets/75575-1.jpg",
      "medium":"/crop/medium/sets/75575-1.jpg",
      "large":"/crop/large/sets/75575-1.jpg",
      "teaser":"/crop/teaser/sets/75575-1.jpg",
      "teaser_compact":"/crop/teaser_compact/sets/75575-1.jpg",
      "generic140xn":"/crop/generic140xn/sets/75575-1.jpg",
      "generic200xn":"/crop/generic200xn/sets/75575-1.jpg",
      "genericnx400":"/crop/genericnx400/sets/75575-1.jpg",
      "original":"/crop/original/sets/75575-1.jpg"
   },
   "reviews":[
      {
         "review_url":"https://brickset.com/reviews/set-75575-1",
         "snippet":"None",
         "review_amount":"1",
         "rating_original":"3.9",
         "rating_converted":"78.00",
         "author_name":"None",
         "embed":"None"
      }
   ]
}
```

### LEGO's website
LEGO's website offers a lot of information about their products. This includes general product information, set rating, the price and more. This information can be scraped from the website using a library like BeautifulSoup.

> [!WARNING]  
> Scraping the LEGO website is at your own risk. Your IP address might also be blocked if you scrape too much data in too short a time.
