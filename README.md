SARIG missing lithology predictions 
==============================

#This repo is still under construction#

The Geological Survey of South Australia's SA Geodata database (back end to SARIG: https://map.sarig.sa.gov.au/) hosts a repository of the states geological data including lithology and geochemistry data.

These notebooks are an application of an ML protolith prediction application to add a lithology to a rock sample that does not have any lithology recorded
against it in SA Geodata using whole rock geochmistry (https://github.com/RADutchie/Rock_protolith_predictor). 

Data located at....

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
        ├── SARIG_procesed_input_data.csv   
        └── SARIG_predicted_lith_data.csv           
        
           
    
--------