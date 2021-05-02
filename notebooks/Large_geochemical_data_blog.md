# Using Python to handle large geochemical datasets
The Geological Survey of South Australia (GSSA) holds a wealth of data in its geoscientific database SA Geodata. SA Geodata is the primary repository for geoscientific information in South Australia and contains open source data collected from a variety of sources. 

The SA Geodata database contains over 10 Gb of geochemical data. That's a lot of chemistry. Explorers often request extracts of this data set, but then find it a challenge to handle all that data. Because of the size and amount of data,  programs like Excel wont even open the file, and if the extract is small enough to open, explorers often find the format of the data a challenge. Generally, people like to use tidy data for analysis, where each row in a table represents all the data about a single sample. But the data exports are in a long format where each row represents a single data point.   

In this blog, I'll show a simple method using python and the Dask and Pandas libraries to query and extract the desired elements, and then quickly pivot them into the desired tidy structure. This example uses a copy of the SA Geodata geochemical data set provided for the Unearthed ExploreSA: Gawler Challenge and is valid as at February 2020.

## When there's too much data
[Dask](https://dask.org/) is an open source library for parallel computing in python. It can be used to parallelise computations across multiple machines, or larger-than-memory data sets on a single machine. It's also tightly integrated with the python PyData stack including Pandas and Numpy, sharing a vary similar API and making working with large DataFrames easy. 

From the Dask website: "A Dask DataFrame is a large parallel DataFrame composed of many smaller Pandas DataFrames, split along the index. These Pandas DataFrames may live on disk for larger-than-memory computing on a single machine, or on many different machines in a cluster."

Most operations in Dask are 'lazy', which means they don't actually evaluate anything until you explicitly ask for it. To return a computed result, you need to call the `compute` method and the results need to be able to fit in memory as this will produce an out put or convert a Dask DataFrame into a Pandas DataFrame. 

Lets work through an example to see how this all works. First we need to import the required libraries:

```python
import dask.dataframe as dd
import pandas as pd
import numpy as np
import re
```

## Loading the dataset
Because of the similar API between Dask and Pandas, loading the dataset is exactly the same as we would using Pandas, except we call Dask (`dd`) instead of Pandas (`pd`):


```python
ddf = dd.read_csv(r'D:\Unearthed_SARIG_Data_Package\SARIG_Data_Package\sarig_rs_chem_exp.csv', 
                    dtype={'LITHO_CONF': 'object', 'STRAT_CONF': 'object'})
```
I found after some trial and error that I needed to force the data types for the 'LITHO_CONF' and 'STRAT_CONF' columns, because Dask incorrectly guessed them as integers. That's why in the above code I have explicitly provided the data type (`dtype`) in the load call. 

Next we can do some simple interrogation of the data like looking at the column names and using the `head()` method to view the top 5 rows of data:

```python
ddf.columns
```




    Index(['SAMPLE_NO', 'SAMPLE_SOURCE_CODE', 'SAMPLE_SOURCE', 'ROCK_GROUP_CODE',
           'ROCK_GROUP', 'LITHO_CODE', 'LITHO_CONF', 'LITHOLOGY_NAME',
           'LITHO_MODIFIER', 'MAP_SYMBOL', 'STRAT_CONF', 'STRAT_NAME',
           'COLLECTED_BY', 'COLLECTORS_NUMBER', 'COLLECTED_DATE',
           'DRILLHOLE_NUMBER', 'DH_NAME', 'DH_DEPTH_FROM', 'DH_DEPTH_TO',
           'SITE_NO', 'EASTING_GDA2020', 'NORTHING_GDA2020', 'ZONE_GDA2020',
           'LONGITUDE_GDA2020', 'LATITUDE_GDA2020', 'LONGITUDE_GDA94',
           'LATITUDE_GDA94', 'SAMPLE_ANALYSIS_NO', 'OTHER_ANALYSIS_ID',
           'ANALYSIS_TYPE_DESC', 'LABORATORY', 'CHEM_CODE', 'VALUE', 'UNIT',
           'CHEM_METHOD_CODE', 'CHEM_METHOD_DESC'],
          dtype='object')




```python
ddf.head(5)
```




<div>
<style scoped>
    .DataFrame tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .DataFrame tbody tr th {
        vertical-align: top;
    }

    .DataFrame thead th {
        text-align: right;
    }
</style>
<table border="1" class="DataFrame">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SAMPLE_NO</th>
      <th>SAMPLE_SOURCE_CODE</th>
      <th>SAMPLE_SOURCE</th>
      <th>ROCK_GROUP_CODE</th>
      <th>ROCK_GROUP</th>
      <th>LITHO_CODE</th>
      <th>LITHO_CONF</th>
      <th>LITHOLOGY_NAME</th>
      <th>LITHO_MODIFIER</th>
      <th>MAP_SYMBOL</th>
      <th>STRAT_CONF</th>
      <th>STRAT_NAME</th>
      <th>COLLECTED_BY</th>
      <th>COLLECTORS_NUMBER</th>
      <th>COLLECTED_DATE</th>
      <th>DRILLHOLE_NUMBER</th>
      <th>DH_NAME</th>
      <th>DH_DEPTH_FROM</th>
      <th>DH_DEPTH_TO</th>
      <th>SITE_NO</th>
      <th>EASTING_GDA2020</th>
      <th>NORTHING_GDA2020</th>
      <th>ZONE_GDA2020</th>
      <th>LONGITUDE_GDA2020</th>
      <th>LATITUDE_GDA2020</th>
      <th>LONGITUDE_GDA94</th>
      <th>LATITUDE_GDA94</th>
      <th>SAMPLE_ANALYSIS_NO</th>
      <th>OTHER_ANALYSIS_ID</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>LABORATORY</th>
      <th>CHEM_CODE</th>
      <th>VALUE</th>
      <th>UNIT</th>
      <th>CHEM_METHOD_CODE</th>
      <th>CHEM_METHOD_DESC</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>589</td>
      <td>RO</td>
      <td>Rock outcrop / float</td>
      <td>SSA</td>
      <td>Sediment Siliciclastic Arenite</td>
      <td>SDST</td>
      <td>NaN</td>
      <td>Sandstone</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>MIRAMS, R.C.</td>
      <td>600/1</td>
      <td>13/12/1960</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>128096</td>
      <td>513602.61</td>
      <td>7.006e+06</td>
      <td>52</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>158</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>NaN</td>
      <td>Ti</td>
      <td>0.15</td>
      <td>%</td>
      <td>AES</td>
      <td>AES</td>
    </tr>
    <tr>
      <td>1</td>
      <td>589</td>
      <td>RO</td>
      <td>Rock outcrop / float</td>
      <td>SSA</td>
      <td>Sediment Siliciclastic Arenite</td>
      <td>SDST</td>
      <td>NaN</td>
      <td>Sandstone</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>MIRAMS, R.C.</td>
      <td>600/1</td>
      <td>13/12/1960</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>128096</td>
      <td>513602.61</td>
      <td>7.006e+06</td>
      <td>52</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>158</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>NaN</td>
      <td>Ag</td>
      <td>500</td>
      <td>ppb</td>
      <td>AES</td>
      <td>AES</td>
    </tr>
    <tr>
      <td>2</td>
      <td>589</td>
      <td>RO</td>
      <td>Rock outcrop / float</td>
      <td>SSA</td>
      <td>Sediment Siliciclastic Arenite</td>
      <td>SDST</td>
      <td>NaN</td>
      <td>Sandstone</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>MIRAMS, R.C.</td>
      <td>600/1</td>
      <td>13/12/1960</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>128096</td>
      <td>513602.61</td>
      <td>7.006e+06</td>
      <td>52</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>158</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>NaN</td>
      <td>Ba</td>
      <td>1200</td>
      <td>ppm</td>
      <td>AES</td>
      <td>AES</td>
    </tr>
    <tr>
      <td>3</td>
      <td>589</td>
      <td>RO</td>
      <td>Rock outcrop / float</td>
      <td>SSA</td>
      <td>Sediment Siliciclastic Arenite</td>
      <td>SDST</td>
      <td>NaN</td>
      <td>Sandstone</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>MIRAMS, R.C.</td>
      <td>600/1</td>
      <td>13/12/1960</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>128096</td>
      <td>513602.61</td>
      <td>7.006e+06</td>
      <td>52</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>158</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>NaN</td>
      <td>Co</td>
      <td>2</td>
      <td>ppm</td>
      <td>AES</td>
      <td>AES</td>
    </tr>
    <tr>
      <td>4</td>
      <td>589</td>
      <td>RO</td>
      <td>Rock outcrop / float</td>
      <td>SSA</td>
      <td>Sediment Siliciclastic Arenite</td>
      <td>SDST</td>
      <td>NaN</td>
      <td>Sandstone</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>MIRAMS, R.C.</td>
      <td>600/1</td>
      <td>13/12/1960</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>128096</td>
      <td>513602.61</td>
      <td>7.006e+06</td>
      <td>52</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>129.137</td>
      <td>-27.070</td>
      <td>158</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>NaN</td>
      <td>Cr</td>
      <td>1000</td>
      <td>ppm</td>
      <td>AES</td>
      <td>AES</td>
    </tr>
  </tbody>
</table>
</div>

For this exercise, we are going to create a tidy dataset (wide format data) which only contains the major elements for rock samples. In the table above we can see there is a column for 'SAMPLE_SOURCE'. To see how many different sample types, and how many records for each, we can compute the value counts for each. As this is evaluating data, we need to call the `compute` method: 


```python
ddf['SAMPLE_SOURCE'].value_counts().compute()
```




    Drill cuttings                                                  10956887
    Sawn half drill core                                             7256976
    Drill core                                                       6533477
    Calcrete                                                         2154614
    Soil                                                              989296
    Drilled interval rock sample, type unspecified                    970508
    Rock outcrop / float                                              537537
    Sawn quarter drill core                                           237103
    Drillhole log data - used in calculating values                   212746
    Stream sediment                                                   164251
    Auger sample from near surface                                    105514
    Vegetation                                                         71018
    Core sludge                                                        56217
    A full face slice of core                                          52531
    Gravel                                                             37740
    Pulp / powdered rock of known origin, typically a lab return       16428
    Magnetic lag fraction                                              10135
    Duplicate: a split from an existing sample.                         8646
    Sample specifically for bulk leach extractable gold analysis        7826
    Aircore: consolidated sample from aircore drilling method           7488
    Analytical standard check sample                                    6358
    Mine rock sample                                                    6203
    Channel (linear rock chip/outcrop sampling)                         4453
    Rock outcrop sample                                                 4086
    Lag sample, surface sample consisting of gravelly material.         3918
    Costean (trench)                                                    3746
    Filleted, shaved or ground core sample                              3478
    Mine mullock / well spoil                                           3086
    Loam                                                                2679
    Mine stockpile                                                      2248
    Lake floor sediment                                                 1799
    Excrement of animals                                                 772
    Mine tailings                                                        347
    Rock float sample (not in situ)                                      335
    Drillhole                                                            332
    Smelter slag                                                         293
    Rock subcrop sample (in situ, not attached to bedrock)                67
    Single mineral                                                        40
    Bulk (high volume) sample, diamond exploration                        19
    Core Library archival rock, original source unknown                   12
    Name: SAMPLE_SOURCE, dtype: int64


We can see above that there are a lot of samples that are not from rocks. We can create a new Dask DataFrame and only select samples from 'rock sample' types, excluding things like soils, lag and excrement of animals! Because this is a 'lazy' operation, it can be done on the total dataset and is quick:


```python
ddf_rock_sample = ddf[ddf['SAMPLE_SOURCE'].isin(['Drill cuttings','Sawn half drill core','Drill core','Drilled interval rock sample, type unspecified','Rock outcrop / float','Sawn quarter drill core','Drillhole log data - used in calculating values','A full face slice of core','Pulp / powdered rock of known origin, typically a lab return','Duplicate: a split from an existing sample.','Channel (linear rock chip/outcrop sampling)','Rock outcrop sample','Costean (trench)','Filleted, shaved or ground core sample','Rock float sample (not in situ)','Drillhole','Rock subcrop sample (in situ, not attached to bedrock)'])]
```

Now we just have rock samples, that should have removed a few hundred thousand rows from our dataset, significantly reducing the size. Next we can reduce that further and only grab the samples of interest, here the ones containing major element data.

First we generate a list of elements we want to include. Then we want to select all of the unique SAMPLE_ANALYSIS_NO (the primary identifier for each sample analysis) that contain data for the required major elements:


```python
elements = ['SiO2','Al2O3','TiO2','Fe2O3','MgO','CaO','Na2O','K2O','P2O5','LOI','FeO']
analyses = ddf_rock_sample[ddf_rock_sample.CHEM_CODE.isin(elements)].SAMPLE_ANALYSIS_NO.unique()
```
Now we have identified each sample we want to keep we can compute a Pandas DataFrame for the unique SAMPLE_ANALYSIS_NO with only the required columns we want (such as the sample number, chem code, value and unit. We can drop unnecessary columns like lithology to further reduce the memory size of the dataset). Because this is converting a larger than memory dataset into a more useable sized dataset, this may take a few moments to process:


```python
ddf_major_chem = ddf_rock_sample[ddf_rock_sample.SAMPLE_ANALYSIS_NO.isin(list(analyses))][['SAMPLE_NO','SAMPLE_ANALYSIS_NO',"CHEM_CODE",'VALUE','UNIT','ANALYSIS_TYPE_DESC','CHEM_METHOD_CODE','OTHER_ANALYSIS_ID','ANALYSIS_TYPE_DESC','COLLECTED_DATE']].drop_duplicates()

df_major_chem = ddf_major_chem.compute()
```
Doing the selection this way, selecting by sample number and not just the specific required elements, is potentially a redundant step. We could instead just use the code above where we selected by the unique sample numbers, and instead of computing a Pandas DataFrame containing all the analyses done on those samples, we could have just selected those rows that belonged to major element data by using the 'elements' list instead of the 'analyses' list in the above code. This would allow us to skip the line of code above and some of the code below. Here I've chosen to do it this way to demonstrate a way to select samples based on containing a type of data, in this case major elements.

So, because we've selected by sample, if we look at an example sample below we can see that the Pandas DataFrame contains the major elements we want plus extras. We can also see that there are duplicate analytes analysed by different methods:


```python
df_major_chem[df_major_chem['SAMPLE_ANALYSIS_NO']==2466083]
```




<div>
<style scoped>
    .DataFrame tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .DataFrame tbody tr th {
        vertical-align: top;
    }

    .DataFrame thead th {
        text-align: right;
    }
</style>
<table border="1" class="DataFrame">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SAMPLE_NO</th>
      <th>SAMPLE_ANALYSIS_NO</th>
      <th>CHEM_CODE</th>
      <th>VALUE</th>
      <th>UNIT</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>CHEM_METHOD_CODE</th>
      <th>OTHER_ANALYSIS_ID</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>COLLECTED_DATE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>147316</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Ga</td>
      <td>30</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147317</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>La</td>
      <td>26.5</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147318</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Mo</td>
      <td>0.5</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147319</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Ni</td>
      <td>24</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147320</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>P</td>
      <td>600</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147321</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>P2O5</td>
      <td>0.137</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147322</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Pb</td>
      <td>4</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147323</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>S</td>
      <td>0.01</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147324</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Sb</td>
      <td>2.5</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147325</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Sc</td>
      <td>8</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147326</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Sr</td>
      <td>35</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147327</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Te</td>
      <td>0.025</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147328</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Th</td>
      <td>20</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147329</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Ti</td>
      <td>0.19</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147330</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Tl</td>
      <td>5</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147331</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>W</td>
      <td>5</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147332</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Y</td>
      <td>47.4</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17613</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Al2O3</td>
      <td>12.9</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17614</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Au</td>
      <td>0.005</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>Au-TL43</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17615</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>CaO</td>
      <td>2.39</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17616</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>K2O</td>
      <td>5.05</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17617</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>MgO</td>
      <td>10.2</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17618</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Na2O</td>
      <td>0.217</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17619</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>SiO2</td>
      <td>53.8</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31118</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Ag</td>
      <td>0.25</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31119</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Al</td>
      <td>7.05</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31120</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>As</td>
      <td>2.5</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31121</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Ba</td>
      <td>1240</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31122</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Be</td>
      <td>7.2</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31123</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Bi</td>
      <td>1</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31124</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Cd</td>
      <td>0.25</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31125</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Ce</td>
      <td>52.9</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31126</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Co</td>
      <td>28</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31127</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Cr</td>
      <td>21</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31128</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Cu</td>
      <td>15</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31129</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Fe</td>
      <td>6.27</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31130</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>U</td>
      <td>10.3</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>31131</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>U3O8</td>
      <td>12</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-XRF21n</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116463</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Ca</td>
      <td>1.76</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116464</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Fe</td>
      <td>6.43</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116465</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>K</td>
      <td>4.24</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116466</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Mg</td>
      <td>6.51</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116467</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Mn</td>
      <td>1.01e+03</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116468</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Na</td>
      <td>0.09</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116469</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>V</td>
      <td>45</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>116470</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Zn</td>
      <td>55</td>
      <td>ppm</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-ICP61</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147444</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>LOI</td>
      <td>5.69</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-GRA05</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
  </tbody>
</table>
</div>



Our next step is to create a new DataFrame with only the required major element rows, dropping the additional trace and rare earth element data for this example. We do this by using our elements list and selecting only rows that have those elements in the 'CHEM_CODE' column: 


```python
df_major_elements = df_major_chem[df_major_chem['CHEM_CODE'].isin(elements)]
```
We can check that we only have the major elements now by checking that same sample again: 

```python
df_major_elements[df_major_elements['SAMPLE_ANALYSIS_NO']==2466083]
```




<div>
<style scoped>
    .DataFrame tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .DataFrame tbody tr th {
        vertical-align: top;
    }

    .DataFrame thead th {
        text-align: right;
    }
</style>
<table border="1" class="DataFrame">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SAMPLE_NO</th>
      <th>SAMPLE_ANALYSIS_NO</th>
      <th>CHEM_CODE</th>
      <th>VALUE</th>
      <th>UNIT</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>CHEM_METHOD_CODE</th>
      <th>OTHER_ANALYSIS_ID</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>COLLECTED_DATE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>147321</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>P2O5</td>
      <td>0.137</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS62s</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17613</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Al2O3</td>
      <td>12.9</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17615</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>CaO</td>
      <td>2.39</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17616</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>K2O</td>
      <td>5.05</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17617</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>MgO</td>
      <td>10.2</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17618</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>Na2O</td>
      <td>0.217</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>17619</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>SiO2</td>
      <td>53.8</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-MS41</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
    <tr>
      <td>147444</td>
      <td>2958572</td>
      <td>2466083</td>
      <td>LOI</td>
      <td>5.69</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>ME-GRA05</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>31/05/2012</td>
    </tr>
  </tbody>
</table>
</div>

As I mentioned above, some of the samples have duplicate analyses by different methods. To check on some of them we can look for duplicate CHEM_CODE and SAMPLE_ANALYSIS_NO fields. 


```python
dup_analysis_samples = df_major_chem[df_major_chem.duplicated(subset=['SAMPLE_ANALYSIS_NO','CHEM_CODE'],keep=False)].sort_values(['SAMPLE_ANALYSIS_NO','CHEM_CODE']) 
```


```python
dup_analysis_samples.head(6)
```


<div>
<style scoped>
    .DataFrame tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .DataFrame tbody tr th {
        vertical-align: top;
    }

    .DataFrame thead th {
        text-align: right;
    }
</style>
<table border="1" class="DataFrame">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SAMPLE_NO</th>
      <th>SAMPLE_ANALYSIS_NO</th>
      <th>CHEM_CODE</th>
      <th>VALUE</th>
      <th>UNIT</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>CHEM_METHOD_CODE</th>
      <th>OTHER_ANALYSIS_ID</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>COLLECTED_DATE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>164713</td>
      <td>969</td>
      <td>776</td>
      <td>Al2O3</td>
      <td>21.36</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>23/11/1984</td>
    </tr>
    <tr>
      <td>164724</td>
      <td>969</td>
      <td>776</td>
      <td>Al2O3</td>
      <td>24.87</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>23/11/1984</td>
    </tr>
    <tr>
      <td>164719</td>
      <td>969</td>
      <td>776</td>
      <td>CaO</td>
      <td>12.16</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>23/11/1984</td>
    </tr>
    <tr>
      <td>164730</td>
      <td>969</td>
      <td>776</td>
      <td>CaO</td>
      <td>12.86</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>23/11/1984</td>
    </tr>
    <tr>
      <td>164715</td>
      <td>969</td>
      <td>776</td>
      <td>Fe2O3</td>
      <td>2.38</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>23/11/1984</td>
    </tr>
    <tr>
      <td>164726</td>
      <td>969</td>
      <td>776</td>
      <td>Fe2O3</td>
      <td>0.57</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF</td>
      <td>NaN</td>
      <td>GEOCHEMISTRY</td>
      <td>23/11/1984</td>
    </tr>
  </tbody>
</table>
</div>

To handle these duplicates properly we would need to look at the 'CHEM_METHOD_CODE' and the labs and decide which to keep. For this example we'll just create a new DataFrame and drop one of the duplicate analyses: 


```python
df_major_elements_tmp1 = df_major_elements.sort_values(['SAMPLE_ANALYSIS_NO','CHEM_CODE','CHEM_METHOD_CODE']).drop_duplicates(subset=['SAMPLE_ANALYSIS_NO','CHEM_CODE'],keep='last')
```

Our subset of major element data now consists of just under 3 million individual analyses. But the data is not all numeric and contains a number of other symbols such as '<' and '-'. We need to find what non-numeric symbols there are and then remove them. But we also need to note that the '<' symbol is the deliminator for analyses below detection limit (BDL). In this case to handle BDL values we will replace the BDL value with half of the reported detection limit.

First we can use regular expressions to find what non-numeric characters are in the element value column. 


```python
sym_find_list = [re.findall(r'\D', str(i)) for i in df_major_elements_tmp1['VALUE']]
unique_symbols = set([item for sublist in sym_find_list for item in sublist])
unique_symbols
```




    {'-', '.', '<', '>'}


We can then use Pandas string methods, like `str.contains` to remove them:

```python
df_major_elements_tmp1.drop(df_major_elements_tmp1[df_major_elements_tmp1['VALUE'].str.contains('>',na=False, regex=False)].index, inplace=True)
```

The '-' symbol is an interesting one as it can potentially be a legitimate symbol for things like LOI. But looking at the samples that contain '-' we can see that in some instances it is used to represent a range of values, like 0-10. To deal with this case (only 2 samples in our dataset) we can explicitly convert them to a different value, such as 10

```python
df_major_elements_tmp1[df_major_elements_tmp1['VALUE'].str.contains(r'-',na=False,regex=False)].tail()
```




<div>
<style scoped>
    .DataFrame tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .DataFrame tbody tr th {
        vertical-align: top;
    }

    .DataFrame thead th {
        text-align: right;
    }
</style>
<table border="1" class="DataFrame">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SAMPLE_NO</th>
      <th>SAMPLE_ANALYSIS_NO</th>
      <th>CHEM_CODE</th>
      <th>VALUE</th>
      <th>UNIT</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>CHEM_METHOD_CODE</th>
      <th>OTHER_ANALYSIS_ID</th>
      <th>ANALYSIS_TYPE_DESC</th>
      <th>COLLECTED_DATE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>14320</td>
      <td>1703219</td>
      <td>1692890</td>
      <td>LOI</td>
      <td>-0.32</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>LOI</td>
      <td>562.0/0910907</td>
      <td>GEOCHEMISTRY</td>
      <td>05/10/2009</td>
    </tr>
    <tr>
      <td>14334</td>
      <td>1703221</td>
      <td>1692892</td>
      <td>LOI</td>
      <td>-0.16</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>LOI</td>
      <td>562.0/0910907</td>
      <td>GEOCHEMISTRY</td>
      <td>05/10/2009</td>
    </tr>
    <tr>
      <td>16070</td>
      <td>1703222</td>
      <td>1692893</td>
      <td>LOI</td>
      <td>-0.35</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>LOI</td>
      <td>562.0/0910907</td>
      <td>GEOCHEMISTRY</td>
      <td>05/10/2009</td>
    </tr>
    <tr>
      <td>95053</td>
      <td>2457789</td>
      <td>1988166</td>
      <td>LOI</td>
      <td>&lt;0-10</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF78S</td>
      <td>WM111749</td>
      <td>GEOCHEMISTRY</td>
      <td>07/10/2008</td>
    </tr>
    <tr>
      <td>59340</td>
      <td>2458844</td>
      <td>1989221</td>
      <td>LOI</td>
      <td>&lt;0-10</td>
      <td>%</td>
      <td>GEOCHEMISTRY</td>
      <td>XRF78S</td>
      <td>WM111749</td>
      <td>GEOCHEMISTRY</td>
      <td>11/09/2008</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_major_elements_tmp1.loc[df_major_elements_tmp1['VALUE'] == '<0-10','VALUE'] = '<10'
```

Now we can deal with the BDL values represented by '<'. There are a number of ways to deal with BDL values, but here we will just convert them to half the reported detection limit.

We do that by first making a flag column identifying which analyses are BDL. Then we can strip the '<' character and finally divide just those rows that have the flag by 2: 


```python
# create a flag column to identify BDL values
df_major_elements_tmp1['BDL'] = 0
df_major_elements_tmp1.loc[df_major_elements_tmp1['VALUE'].str.contains('<',na=False,regex=False), 'BDL'] = 1

```


```python
# remove < in vlues col
df_major_elements_tmp1['VALUE'] = df_major_elements_tmp1['VALUE'].astype(str).str.replace("<", "").astype(float)
```


```python
#convert BDL units to half reported limit
df_major_elements_tmp1.loc[df_major_elements_tmp1['BDL'] == 1,'VALUE'] = df_major_elements_tmp1.loc[df_major_elements_tmp1['BDL'] == 1,'VALUE'] /2
```
From here it's possible to do a number of further validation checks on the data, such as checking that the values fall within a realistic range etc. But for this example we'll assume the data from here is ready to go.

The last thing we need to do is convert the cleaned chemical data from long format to a tidy data wide format, and convert data types from object to float. We can do this by pivoting the DataFrame (when you pivot the data, it creates a multilevel header. To fix this we simply rename the columns of the table, flattening it back to a single header row):


```python
df_major_chem_wide = df_major_elements_tmp1.pivot(index='SAMPLE_ANALYSIS_NO', values=['VALUE'],columns='CHEM_CODE').sort_values('CHEM_CODE',axis=1)

df_major_chem_wide.columns = ['Al2O3','CaO','Fe2O3','FeO','K2O','LOI','MgO','Na2O','P2O5','SiO2','TiO2']
```


```python
df_major_chem_wide.info()
```

    <class 'Pandas.core.frame.DataFrame'>
    Int64Index: 321523 entries, 122 to 2476035
    Data columns (total 20 columns):
    (VALUE, Al2O3)    189183 non-null float64
    (VALUE, CaO)      189758 non-null float64
    (VALUE, Fe2O3)    61647 non-null float64
    (VALUE, FeO)      4665 non-null float64
    (VALUE, K2O)      186116 non-null float64
    (VALUE, LOI)      156535 non-null float64
    (VALUE, MgO)      190418 non-null float64
    (VALUE, Na2O)     171249 non-null float64
    (VALUE, P2O5)     211858 non-null float64
    (VALUE, SiO2)     192035 non-null float64
    (VALUE, TiO2)     151620 non-null float64
    dtypes: float64(20)
    memory usage: 61.5 MB
    


```python
df_major_chem_wide.head()
```




<div>
<style scoped>
    .DataFrame tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .DataFrame tbody tr th {
        vertical-align: top;
    }

    .DataFrame thead th {
        text-align: right;
    }
</style>
<table border="1" class="DataFrame">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Al2O3</th>
      <th>CaO</th>
      <th>Fe2O3</th>
      <th>FeO</th>
      <th>K2O</th>
      <th>LOI</th>
      <th>MgO</th>
      <th>Na2O</th>
      <th>P2O5</th>
      <th>SiO2</th>
      <th>TiO2</th>
    </tr>
    <tr>
      <th>SAMPLE_ANALYSIS_NO</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>122</td>
      <td>14.5</td>
      <td>8.55</td>
      <td>11.10</td>
      <td>NaN</td>
      <td>1.39</td>
      <td>1.25</td>
      <td>4.62</td>
      <td>2.26</td>
      <td>0.09</td>
      <td>55.5</td>
      <td>0.84</td>
    </tr>
    <tr>
      <td>124</td>
      <td>14.9</td>
      <td>8.95</td>
      <td>10.30</td>
      <td>NaN</td>
      <td>1.38</td>
      <td>1.85</td>
      <td>4.86</td>
      <td>2.10</td>
      <td>0.07</td>
      <td>55.0</td>
      <td>0.85</td>
    </tr>
    <tr>
      <td>126</td>
      <td>14.9</td>
      <td>5.95</td>
      <td>11.10</td>
      <td>NaN</td>
      <td>2.18</td>
      <td>2.28</td>
      <td>5.20</td>
      <td>4.26</td>
      <td>0.09</td>
      <td>53.7</td>
      <td>0.85</td>
    </tr>
    <tr>
      <td>128</td>
      <td>15.9</td>
      <td>8.70</td>
      <td>8.40</td>
      <td>NaN</td>
      <td>1.32</td>
      <td>4.42</td>
      <td>4.24</td>
      <td>2.08</td>
      <td>0.09</td>
      <td>54.4</td>
      <td>0.90</td>
    </tr>
    <tr>
      <td>130</td>
      <td>18.6</td>
      <td>3.80</td>
      <td>1.94</td>
      <td>NaN</td>
      <td>6.45</td>
      <td>3.44</td>
      <td>1.08</td>
      <td>2.18</td>
      <td>0.02</td>
      <td>60.8</td>
      <td>1.06</td>
    </tr>
  </tbody>
</table>
</div>

Finally we can export our new, customised, tidy format dataset using the Pandas `to_csv` method:

```python
df_major_chem_wide.to_csv(r'D:\SARIG_major_els_v1.csv')
```

## Summary 

In this example we have taken a very large geochemical dataset, more than 10Gb worth of data, and turned it into a useable size of less than 65Mb. Using Dask we were able to deal with more data than my computer could handle in memory, filter it down to just the sample types and elements that we wanted (still over 300,000 samples mind you), and then convert it into the usual tidy data wide table format most of us are used to dealing with.

I hope you find this example useful for your own efforts to deal with large geological datasets. With a little bit of code and the right python libraries anyone can move away from Excel and start using larger than memory datasets. 

Please check out my other content at [GeoDataAnalytics.net](https://geodataanalytics.net), and get in touch if you have any questions or comments. And make sure you check out all the amazing work and freely available data from the [Geological Survey of South Australia](https://energymining.sa.gov.au/minerals/geoscience/geological_survey) and [SARIG](https://map.sarig.sa.gov.au/). This example forms part of a larger exploration and transformation of this dataset for application in a lithology prediction machine learning model and can be found on [GitHub](https://github.com/RADutchie/SA-geology-litho-predictions).
