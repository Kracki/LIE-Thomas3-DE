## Diabetes Diagnosis Prediction

The goal of this project is to create a binary classifier to help for diabetes diagnosis.

### Steps

**1. Preprocessing**

- Create spark session
- load data
- clean missing values
- drop unnecessary columns

**2. Feature engineering**

from the useful columns create a vector using the VectorAssembler
Split between test and train 

**3. model training**

Train different models to test their performances using ml lib in spark

**4. Evaluation**

Compare the different models using accuracy, precision, recall.


**5. Nice to have**

Create a server sending the data and connect to it instead of reading the csv file.
