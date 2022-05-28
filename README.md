# data-bot
Just a simple Selenium based bot to scrape geographic data from Geotab's Open Driving Data (Ignition).

Basic functionality will load up the Ignition webpage, login using credentials stored in a text file in a folder named "creds".

Example structure of ```ignition_creds.txt```.
Store it in a subdirectory called ```creds``` alongside ```src```

```
your_email@domain.com
your_password
```

I am utilizing a local Postgres server stored on my machine to store the scraped data with ```sqlalchemy``` and ```pandas```
