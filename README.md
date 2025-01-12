# petroineos-de

This project contains an ETL pipeline which stars from web scrapping to create final resultant dataset. 
1. Downloading any new xlsx file updated on the website - https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends
2. Each time we download the file we first download into a landing zone the get them processed by our pipeline
3. Once processed we place them into backup folder for later usage
4. The resultant output gets saved in an output folder.

![assignment1](https://github.com/user-attachments/assets/c5df40ee-555a-43dc-8f18-73b189fd702f)

In oder to use the pipeline the only thing is to change is to update the `project_location` in the constants section and execute