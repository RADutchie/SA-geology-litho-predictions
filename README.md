SARIG missing lithology predictions 
==============================


The Geological Survey of South Australia's SA Geodata database (back end to SARIG: https://map.sarig.sa.gov.au/) is the primary repository for geoscientific information in South Australia and contains data collected: from research and fieldwork conducted by GSSA staff; by mineral exploration companies who are required to submit most of the data they collect during their exploration programs to the state government, and; data collected by research institutions from analysis of core and rock samples held in the South Australian Core Library.  

Because this database hosts a significant amount of legacy data, and data collected from various sources, in many instances the data is incomplete or missing key fields. Lithology is one such data type that is often missing from samples collected for other types of analysis, such as for lithogeochemistry.

These notebooks are an application of an ML protolith prediction application to add a lithology to a rock sample that does not have any lithology recorded
against it in SA Geodata using whole rock geochemistry (https://github.com/RADutchie/Rock_protolith_predictor). They are an example of how we might clean and prepare a dataset for application to the model and then apply the model to provide additional insights and fill in missing data on legacy samples. In this case we can generate 148,954 new modeled lithology labels for rock samples held in the database.

The SARIG Data Package is an extract from the GSSA's SA Geodata. This snapshot of the database was provided for the ExploreSA: Gawler Challenge and is valid as at Feburary 2020.

To run the notebooks locally
------------
* Setup a virtualenv using conda or venv running Python 3.7
* `pip3 install -r requirements.txt` 
* Clone the notebooks folder

Project Organization
------------

    ├── README.md
    ├── requirements.txt  
    ├── notebooks
    │   ├── SARIG_chem_load_clean.ipynb      
    │   └── SARIG_lithochem_lithology_predictions.ipynb
    └── data              
        ├── SARIG_major_els_v1.csv              <--- Cleaned and processed model input data
        ├── SARIG_predicted_lith_validated_data.csv   
        ├── SARIG_predicted_lith_metamorphic_data.csv           
        └── SARIG_predicted_lith_unlabeled_data.csv
           
    
--------
