# Slide 14 Questions

### 1. Understand the differences between Google Dataflow and Dataproc
* __Google Dataflow:__ place to run Apache Beam based jobs, without the need to address the technicalities of running jobs on a cluster (such as work balancing and scaling), since  this is automatically done for you. With Google Dataflow you focus on the logical computation rather than how the runner works. Additionally, Dataflow provides base templates that simplify common tasks. Beams has the downside of only supporting _Python 2.7_.
* __Dataproc:__ provides you with a Hadoop Cluster and access to Apache Hadoop / Spark ecosystem. Indicated when the manual provisioning of clusters is necessary (as seen before, GD does it automatically) OR when there are dependencies to tools belonging to the aforementioned ecosystem.

### 2. Understand how a pipeline is created


### 3. Understand the learning task


### 4. Understand the contents of each script in the pipeline
* `data-extractor.py`: Downloads the SDF files.

* `preprocess.py`: Parses the SDF files to then count how many Carbon, Hydrogen, Oxygen and Nitrogen atoms a molecule has and then normalizes that values (from 0 to 1). Uses `tf.Transform` to find the minimum and maximum number of counts (full pass over the dataset). The process up to this phase can be nominated __Feature Extraction__. Splits the dataset into training (80%) and test (20%).

* `trainer/task.py`: Loads the data that was processed in the preprocessing phase and then uses the training dataset to train the model, and then uses the evaluation dataset to verify that the model accurately predicts molecular energy given some of the molecule’s properties.

* `predict.py`: Provide the model with inputs and it will make predictions. The pipeline can act as either a **batch pipeline** or a **streaming pipeline**. Batch pipeline is indicated when there are a large amount of predictions and the user can wait for all of them to finish. Streaming pipeline is indicated when the User is sending sporadic predictions and wants to get the results as soon as possible. The batch and streaming pipeline differ on the source and sink interactions.

* `publisher.py`: Parses SDF files from a directory and publishes them to the inputs topic.

* `subscriber.py`: Listens for prediction results and logs them.

### 5. Understand how to use transformations and estimators in Tensorflow


### 6. Run the pipeline as is locally (run-local) and in the cloud (run-cloud) (are there any differences in performance?)


### 7. Vary the max-data-files parameter with values 10, 100, 1000
* `./run-local --max-data-files 10`:
* `./run-local --max-data-files 100`:
* `./run-local --max-data-files 1000`:

### 8. Modify this program to include the actual ENERGY of each molecule in the predictions file


### 9. Modify this program to allow for cross-validation


***

### Helpful links:
* https://cloud.google.com/dataflow/docs/samples/molecules-walkthrough
